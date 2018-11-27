use buzhash::BuzHash;
use sha2::{Digest, Sha512};
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::SeekFrom;
use threadpool::ThreadPool;

use archive_reader::*;
use chunker::Chunker;
use chunker_utils::*;
use config::*;
use string_utils::*;

fn fill_from_seed<T, F>(
    mut seed_input: T,
    chunker: Chunker,
    hash_length: usize,
    chunk_hash_set: &mut HashSet<HashBuf>,
    mut result: F,
    pool: &ThreadPool,
) where
    T: Read,
    F: FnMut(&HashBuf, &Vec<u8>),
{
    // Read chunks from seed file.
    // TODO: Should first check if input might be a archive file and then use its chunks as is.
    // If input is an archive also check if chunker parameter
    // matches, otherwise error or warn user?
    // Generate strong hash for a chunk
    let hasher = |data: &[u8]| {
        let mut hasher = Sha512::new();
        hasher.input(data);
        hasher.result().to_vec()
    };

    unique_chunks(&mut seed_input, chunker, hasher, &pool, |hashed_chunk| {
        let hash = &hashed_chunk.hash[0..hash_length].to_vec();
        if chunk_hash_set.contains(hash) {
            result(hash, &hashed_chunk.chunk.data);
            chunk_hash_set.remove(hash);
        }
    }).expect("compress chunks");
}

pub fn run(config: UnpackConfig, pool: ThreadPool) {
    println!("Do unpack ({:?})", config);

    let src_file =
        File::open(&config.input).expect(&format!("failed to open file ({})", config.input));

    let mut archive = ArchiveReader::new(src_file);
    let mut chunks_left = archive.chunk_hash_set();

    // Create or open output file.
    // TODO: Check if the given file is a block device or a regular file.
    // If it is a block device we should not try to change its size,
    // instead ensure that the source size is the same as the block device.
    let mut output_file = OpenOptions::new()
        .write(true)
        .create(config.base.force_create)
        .truncate(config.base.force_create)
        .create_new(!config.base.force_create)
        .open(&config.output)
        .expect(&format!("failed to create file ({})", config.output));

    output_file
        .set_len(archive.source_total_size)
        .expect("resize output file");

    // Setup chunker to use when chunking seed input
    let chunker = Chunker::new(
        1024 * 1024,
        archive.avg_chunk_size as u32,
        archive.min_chunk_size,
        archive.max_chunk_size,
        BuzHash::new(archive.hash_window_size as usize, 0x10324195),
    );

    let mut total_read_from_seed = 0;
    let mut total_from_archive = 0;

    for seed in config.seed_files {
        let seed_file = File::open(&seed).expect(&format!("failed to open file ({})", seed));

        println!("{} chunks missing. Search in {}.", chunks_left.len(), seed);
        fill_from_seed(
            seed_file,
            chunker.clone(),
            archive.hash_length,
            &mut chunks_left,
            |hash, chunk_data| {
                // Got chunk
                println!(
                    "Chunk '{}', size {} read from seed {}",
                    HexSlice::new(hash),
                    size_to_str(&(chunk_data.len())),
                    seed,
                );

                total_read_from_seed += chunk_data.len();

                for offset in archive.chunk_source_offsets(hash) {
                    output_file
                        .seek(SeekFrom::Start(offset as u64))
                        .expect("seek output");
                    output_file.write(&chunk_data).expect("write output");
                }
            },
            &pool,
        );

        if chunks_left.len() == 0 {
            println!("All chunks was found in seed.");
            break;
        }
        println!("{} chunks still missing.", chunks_left.len());
    }

    archive.read_chunk_data(&chunks_left, |chunk| {
        total_from_archive += chunk.data.len();
        output_file
            .seek(SeekFrom::Start(chunk.offset as u64))
            .expect("seek output");
        let wc = output_file.write(&chunk.data).expect("write output");
        if wc != chunk.data.len() {
            panic!("wc != chunk.data.len()");
        }
    });

    println!(
        "Unpacked using {} from seed and {} from archive.",
        size_to_str(&total_read_from_seed),
        size_to_str(&(archive.total_read as usize))
    );
}