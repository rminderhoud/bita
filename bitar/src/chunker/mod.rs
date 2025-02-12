//! Chunker related functions and types.
mod config;
mod fixed_size;
mod rolling_hash;

pub use config::{Config, FilterBits, FilterConfig};
pub use fixed_size::FixedSizeChunker;
pub use rolling_hash::RollingHashChunker;

use bytes::BytesMut;
use core::pin::Pin;
use core::task::{Context, Poll};
use futures_util::stream::Stream;
use std::io;
use tokio::io::{AsyncRead, ReadBuf};

use crate::Chunk;

const CHUNKER_BUF_SIZE: usize = 1024 * 1024;

/// A chunker scans a readable source for chunks and emits them as a stream.
pub trait Chunker {
    fn poll_chunk(&mut self, cx: &mut Context) -> Poll<Option<io::Result<(u64, Chunk)>>>;
}

impl Stream for dyn Chunker + Send + Unpin + '_ {
    type Item = io::Result<(u64, Chunk)>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.poll_chunk(cx)
    }
}

pub(crate) fn refill_read_buf<T>(
    cx: &mut Context,
    want: usize,
    read_buf: &mut BytesMut,
    mut source: &mut T,
) -> Poll<io::Result<usize>>
where
    T: AsyncRead + Unpin,
{
    let mut read_count = 0;
    let before_size = read_buf.len();
    {
        // Use set_len() here instead of resize as we don't care for zeroing the content of buf.
        let new_size = before_size + want;
        if read_buf.capacity() < new_size {
            read_buf.reserve(want);
        }
        unsafe {
            read_buf.set_len(new_size);
        }
    }
    while read_count < want {
        let offset = before_size + read_count;
        let mut buf = ReadBuf::new(&mut read_buf[offset..]);
        let rc = match Pin::new(&mut source).poll_read(cx, &mut buf) {
            Poll::Ready(Ok(())) if buf.filled().is_empty() => break, // EOF
            Poll::Ready(Ok(())) => buf.filled().len(),
            Poll::Ready(Err(err)) => {
                read_buf.resize(before_size + read_count, 0);
                return Poll::Ready(Err(err));
            }
            Poll::Pending => {
                read_buf.resize(before_size + read_count, 0);
                if read_count > 0 {
                    return Poll::Ready(Ok(read_count));
                }
                return Poll::Pending;
            }
        };
        read_count += rc;
    }
    read_buf.resize(before_size + read_count, 0);
    Poll::Ready(Ok(read_count))
}

#[cfg(test)]
mod tests {
    use super::config::{Config, FilterBits, FilterConfig};
    use super::*;
    use core::pin::Pin;
    use core::task::{Context, Poll};
    use futures_util::stream::StreamExt;
    use std::cmp;
    use tokio::io::AsyncRead;

    // The MockSource will return bytes_per_read bytes every other read
    // and Pending every other, to replicate a source with limited I/O.
    struct MockSource {
        data: Vec<u8>,
        offset: usize,
        bytes_per_read: usize,
        pending: bool,
    }
    impl MockSource {
        fn new(data: Vec<u8>, bytes_per_read: usize) -> Self {
            Self {
                data,
                offset: 0,
                bytes_per_read,
                pending: false,
            }
        }
    }
    impl AsyncRead for MockSource {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context,
            buf: &mut ReadBuf,
        ) -> Poll<io::Result<()>> {
            let data_available = self.data.len() - self.offset;
            if data_available == 0 {
                Poll::Ready(Ok(()))
            } else if self.pending {
                self.pending = false;
                Poll::Pending
            } else {
                let read = cmp::min(
                    data_available,
                    cmp::min(buf.initialized().len(), self.bytes_per_read),
                );
                buf.put_slice(&self.data[self.offset..self.offset + read]);
                self.offset += read;
                self.pending = true;
                Poll::Ready(Ok(()))
            }
        }
    }

    #[tokio::test]
    async fn single_byte_per_source_read() {
        for chunker_config in &[
            Config::RollSum(FilterConfig {
                filter_bits: FilterBits(10),
                min_chunk_size: 20,
                max_chunk_size: 600,
                window_size: 10,
            }),
            Config::BuzHash(FilterConfig {
                filter_bits: FilterBits(10),
                min_chunk_size: 20,
                max_chunk_size: 600,
                window_size: 10,
            }),
        ] {
            let source_data: Vec<u8> = {
                let mut seed: usize = 0xa3;
                (0..10000)
                    .map(|v| {
                        seed ^= seed.wrapping_mul(4);
                        (seed ^ v) as u8
                    })
                    .collect()
            };
            let expected_offsets = {
                chunker_config
                    .new_chunker(&mut Box::new(&source_data[..]))
                    .map(|result| {
                        let (offset, _chunk) = result.unwrap();
                        offset
                    })
                    .collect::<Vec<u64>>()
                    .await
            };
            // Only give back a single byte per read from source, should still result in the same
            // result as with unlimited I/O.
            let mut source = MockSource::new(source_data.clone(), 1);
            let offsets = chunker_config
                .new_chunker(&mut source)
                .map(|result| {
                    let (offset, _chunk) = result.unwrap();
                    offset
                })
                .collect::<Vec<u64>>()
                .await;
            assert_eq!(expected_offsets, offsets);
        }
    }
    #[tokio::test]
    async fn zero_data() {
        for chunker_config in &[
            Config::RollSum(FilterConfig {
                filter_bits: FilterBits(5),
                min_chunk_size: 3,
                max_chunk_size: 640,
                window_size: 5,
            }),
            Config::BuzHash(FilterConfig {
                filter_bits: FilterBits(5),
                min_chunk_size: 3,
                max_chunk_size: 640,
                window_size: 5,
            }),
        ] {
            let expected_chunk_offsets: [u64; 0] = [0; 0];
            static SRC: [u8; 0] = [];
            assert_eq!(
                chunker_config
                    .new_chunker(&mut Box::new(&SRC[..]))
                    .map(|result| {
                        let (offset, chunk) = result.unwrap();
                        assert_eq!(chunk.len(), 0);
                        offset
                    })
                    .collect::<Vec<u64>>()
                    .await,
                &expected_chunk_offsets
            );
        }
    }
    #[tokio::test]
    async fn source_smaller_than_hash_window() {
        for chunker_config in &[
            Config::RollSum(FilterConfig {
                filter_bits: FilterBits(5),
                min_chunk_size: 0,
                max_chunk_size: 40,
                window_size: 10,
            }),
            Config::BuzHash(FilterConfig {
                filter_bits: FilterBits(5),
                min_chunk_size: 0,
                max_chunk_size: 40,
                window_size: 10,
            }),
        ] {
            let expected_chunk_offsets: [u64; 1] = [0; 1];
            static SRC: [u8; 5] = [0x1f, 0x55, 0x39, 0x5e, 0xfa];
            assert_eq!(
                chunker_config
                    .new_chunker(&mut Box::new(&SRC[..]))
                    .map(|result| {
                        let (offset, chunk) = result.unwrap();
                        assert_eq!(chunk, Chunk::from(vec![0x1f, 0x55, 0x39, 0x5e, 0xfa]));
                        offset
                    })
                    .collect::<Vec<u64>>()
                    .await,
                &expected_chunk_offsets
            );
        }
    }
    #[tokio::test]
    async fn source_smaller_than_min_chunk() {
        for chunker_config in &[
            Config::RollSum(FilterConfig {
                filter_bits: FilterBits(5),
                min_chunk_size: 10,
                max_chunk_size: 40,
                window_size: 5,
            }),
            Config::BuzHash(FilterConfig {
                filter_bits: FilterBits(5),
                min_chunk_size: 10,
                max_chunk_size: 40,
                window_size: 5,
            }),
        ] {
            let expected_chunk_offsets: [u64; 1] = [0; 1];
            static SRC: [u8; 5] = [0x1f, 0x55, 0x39, 0x5e, 0xfa];
            assert_eq!(
                chunker_config
                    .new_chunker(&mut Box::new(&SRC[..]))
                    .map(|result| {
                        let (offset, chunk) = result.unwrap();
                        assert_eq!(chunk, Chunk::from(vec![0x1f, 0x55, 0x39, 0x5e, 0xfa]));
                        offset
                    })
                    .collect::<Vec<u64>>()
                    .await,
                &expected_chunk_offsets
            );
        }
    }
    #[tokio::test]
    async fn consistency_small_min_chunk_buzhash() {
        let expected_chunk_offsets = vec![
            0, 23, 139, 162, 177, 194, 224, 237, 279, 395, 418, 433, 450, 480, 493, 535, 651, 674,
            689, 706, 736, 749, 791, 907, 930, 945, 962, 992, 1005, 1047, 1163, 1186, 1201, 1218,
            1248, 1261, 1303, 1419, 1442, 1457, 1474, 1504, 1517, 1559, 1675, 1698, 1713, 1730,
            1760, 1773, 1815, 1931, 1954, 1969, 1986, 2016, 2029, 2071, 2187, 2210, 2225, 2242,
            2272, 2285, 2327, 2443, 2466, 2481, 2498, 2528, 2541, 2583, 2699, 2722, 2737, 2754,
            2784, 2797, 2839, 2955, 2978, 2993, 3010, 3040, 3053, 3095, 3211, 3234, 3249, 3266,
            3296, 3309, 3351, 3467, 3490, 3505, 3522, 3552, 3565, 3607, 3723, 3746, 3761, 3778,
            3808, 3821, 3863, 3979, 4002, 4017, 4034, 4064, 4077, 4119, 4235, 4258, 4273, 4290,
            4320, 4333, 4375, 4491, 4514, 4529, 4546, 4576, 4589, 4631, 4747, 4770, 4785, 4802,
            4832, 4845, 4887, 5003, 5026, 5041, 5058, 5088, 5101, 5143, 5259, 5282, 5297, 5314,
            5344, 5357, 5399, 5515, 5538, 5553, 5570, 5600, 5613, 5655, 5771, 5794, 5809, 5826,
            5856, 5869, 5911, 6027, 6050, 6065, 6082, 6112, 6125, 6167, 6283, 6306, 6321, 6338,
            6368, 6381, 6423, 6539, 6562, 6577, 6594, 6624, 6637, 6679, 6795, 6818, 6833, 6850,
            6880, 6893, 6935, 7051, 7074, 7089, 7106, 7136, 7149, 7191, 7307, 7330, 7345, 7362,
            7392, 7405, 7447, 7563, 7586, 7601, 7618, 7648, 7661, 7703, 7819, 7842, 7857, 7874,
            7904, 7917, 7959, 8075, 8098, 8113, 8130, 8160, 8173, 8215, 8331, 8354, 8369, 8386,
            8416, 8429, 8471, 8587, 8610, 8625, 8642, 8672, 8685, 8727, 8843, 8866, 8881, 8898,
            8928, 8941, 8983, 9099, 9122, 9137, 9154, 9184, 9197, 9239, 9355, 9378, 9393, 9410,
            9440, 9453, 9495, 9611, 9634, 9649, 9666, 9696, 9709, 9751, 9867, 9890, 9905, 9922,
            9952, 9965,
        ];
        let mut seed = 0xa3;
        static mut SRC: Vec<u8> = Vec::new();
        for v in 0..10000 {
            seed ^= v;
            unsafe {
                SRC.push((seed & 0xff) as u8);
            }
        }

        let chunk_offsets = Config::BuzHash(FilterConfig {
            filter_bits: FilterBits(5),
            min_chunk_size: 3,
            max_chunk_size: 640,
            window_size: 5,
        })
        .new_chunker(&mut Box::new(unsafe { &SRC[..] }))
        .map(|result| {
            let (offset, _chunk) = result.unwrap();
            offset
        })
        .collect::<Vec<u64>>()
        .await;
        assert_eq!(chunk_offsets, expected_chunk_offsets);
    }
    #[tokio::test]
    async fn consistency_bigger_min_chunk_buzhash() {
        let expected_chunk_offsets = vec![
            0, 132, 216, 388, 472, 644, 728, 900, 984, 1156, 1240, 1412, 1496, 1668, 1752, 1924,
            2008, 2180, 2264, 2436, 2520, 2692, 2776, 2948, 3032, 3204, 3288, 3460, 3544, 3716,
            3800, 3972, 4056, 4228, 4312, 4484, 4568, 4740, 4824, 4996, 5080, 5252, 5336, 5508,
            5592, 5764, 5848, 6020, 6104, 6276, 6360, 6532, 6616, 6788, 6872, 7044, 7128, 7300,
            7384, 7556, 7640, 7812, 7896, 8068, 8152, 8324, 8408, 8580, 8664, 8836, 8920, 9092,
            9176, 9348, 9432, 9604, 9688, 9860, 9944, 10116, 10200, 10372, 10456, 10628, 10712,
            10884, 10968, 11140, 11224, 11396, 11480, 11652, 11736, 11908, 11992, 12164, 12248,
            12420, 12504, 12676, 12760, 12932, 13016, 13188, 13272, 13444, 13528, 13700, 13784,
            13956, 14040, 14212, 14296, 14468, 14552, 14724, 14808, 14980, 15064, 15236, 15320,
            15492, 15576, 15748, 15832, 16004, 16088, 16260, 16344, 16516, 16600, 16772, 16856,
            17028, 17112, 17284, 17368, 17540, 17624, 17796, 17880, 18052, 18136, 18308, 18392,
            18564, 18648, 18820, 18904, 19076, 19160, 19332, 19416, 19588, 19672, 19844, 19928,
            20100, 20184, 20356, 20440, 20612, 20696, 20868, 20952, 21124, 21208, 21380, 21464,
            21636, 21720, 21892, 21976, 22148, 22232, 22404, 22488, 22660, 22744, 22916, 23000,
            23172, 23256, 23428, 23512, 23684, 23768, 23940, 24024, 24196, 24280, 24452, 24536,
            24708, 24792, 24964, 25048, 25220, 25304, 25476, 25560, 25732, 25816, 25988, 26072,
            26244, 26328, 26500, 26584, 26756, 26840, 27012, 27096, 27268, 27352, 27524, 27608,
            27780, 27864, 28036, 28120, 28292, 28376, 28548, 28632, 28804, 28888, 29060, 29144,
            29316, 29400, 29572, 29656, 29828, 29912, 30084, 30168, 30340, 30424, 30596, 30680,
            30852, 30936, 31108, 31192, 31364, 31448, 31620, 31704, 31876, 31960, 32132, 32216,
            32388, 32472, 32644, 32728, 32900, 32984, 33156, 33240, 33412, 33496, 33668, 33752,
            33924, 34008, 34180, 34264, 34436, 34520, 34692, 34776, 34948, 35032, 35204, 35288,
            35460, 35544, 35716, 35800, 35972, 36056, 36228, 36312, 36484, 36568, 36740, 36824,
            36996, 37080, 37252, 37336, 37508, 37592, 37764, 37848, 38020, 38104, 38276, 38360,
            38532, 38616, 38788, 38872, 39044, 39128, 39300, 39384, 39556, 39640, 39812, 39896,
            40068, 40152, 40324, 40408, 40580, 40664, 40836, 40920, 41092, 41176, 41348, 41432,
            41604, 41688, 41860, 41944, 42116, 42200, 42372, 42456, 42628, 42712, 42884, 42968,
            43140, 43224, 43396, 43480, 43652, 43736, 43908, 43992, 44164, 44248, 44420, 44504,
            44676, 44760, 44932, 45016, 45188, 45272, 45444, 45528, 45700, 45784, 45956, 46040,
            46212, 46296, 46468, 46552, 46724, 46808, 46980, 47064, 47236, 47320, 47492, 47576,
            47748, 47832, 48004, 48088, 48260, 48344, 48516, 48600, 48772, 48856, 49028, 49112,
            49284, 49368, 49540, 49624, 49796, 49880, 50052, 50136, 50308, 50392, 50564, 50648,
            50820, 50904, 51076, 51160, 51332, 51416, 51588, 51672, 51844, 51928, 52100, 52184,
            52356, 52440, 52612, 52696, 52868, 52952, 53124, 53208, 53380, 53464, 53636, 53720,
            53892, 53976, 54148, 54232, 54404, 54488, 54660, 54744, 54916, 55000, 55172, 55256,
            55428, 55512, 55684, 55768, 55940, 56024, 56196, 56280, 56452, 56536, 56708, 56792,
            56964, 57048, 57220, 57304, 57476, 57560, 57732, 57816, 57988, 58072, 58244, 58328,
            58500, 58584, 58756, 58840, 59012, 59096, 59268, 59352, 59524, 59608, 59780, 59864,
            60036, 60120, 60292, 60376, 60548, 60632, 60804, 60888, 61060, 61144, 61316, 61400,
            61572, 61656, 61828, 61912, 62084, 62168, 62340, 62424, 62596, 62680, 62852, 62936,
            63108, 63192, 63364, 63448, 63620, 63704, 63876, 63960, 64132, 64216, 64388, 64472,
            64644, 64728, 64900, 64984, 65156, 65240, 65412, 65496, 65668, 65752, 65924, 66008,
            66180, 66264, 66436, 66520, 66692, 66776, 66948, 67032, 67204, 67288, 67460, 67544,
            67716, 67800, 67972, 68056, 68228, 68312, 68484, 68568, 68740, 68824, 68996, 69080,
            69252, 69336, 69508, 69592, 69764, 69848, 70020, 70104, 70276, 70360, 70532, 70616,
            70788, 70872, 71044, 71128, 71300, 71384, 71556, 71640, 71812, 71896, 72068, 72152,
            72324, 72408, 72580, 72664, 72836, 72920, 73092, 73176, 73348, 73432, 73604, 73688,
            73860, 73944, 74116, 74200, 74372, 74456, 74628, 74712, 74884, 74968, 75140, 75224,
            75396, 75480, 75652, 75736, 75908, 75992, 76164, 76248, 76420, 76504, 76676, 76760,
            76932, 77016, 77188, 77272, 77444, 77528, 77700, 77784, 77956, 78040, 78212, 78296,
            78468, 78552, 78724, 78808, 78980, 79064, 79236, 79320, 79492, 79576, 79748, 79832,
            80004, 80088, 80260, 80344, 80516, 80600, 80772, 80856, 81028, 81112, 81284, 81368,
            81540, 81624, 81796, 81880, 82052, 82136, 82308, 82392, 82564, 82648, 82820, 82904,
            83076, 83160, 83332, 83416, 83588, 83672, 83844, 83928, 84100, 84184, 84356, 84440,
            84612, 84696, 84868, 84952, 85124, 85208, 85380, 85464, 85636, 85720, 85892, 85976,
            86148, 86232, 86404, 86488, 86660, 86744, 86916, 87000, 87172, 87256, 87428, 87512,
            87684, 87768, 87940, 88024, 88196, 88280, 88452, 88536, 88708, 88792, 88964, 89048,
            89220, 89304, 89476, 89560, 89732, 89816, 89988, 90072, 90244, 90328, 90500, 90584,
            90756, 90840, 91012, 91096, 91268, 91352, 91524, 91608, 91780, 91864, 92036, 92120,
            92292, 92376, 92548, 92632, 92804, 92888, 93060, 93144, 93316, 93400, 93572, 93656,
            93828, 93912, 94084, 94168, 94340, 94424, 94596, 94680, 94852, 94936, 95108, 95192,
            95364, 95448, 95620, 95704, 95876, 95960, 96132, 96216, 96388, 96472, 96644, 96728,
            96900, 96984, 97156, 97240, 97412, 97496, 97668, 97752, 97924, 98008, 98180, 98264,
            98436, 98520, 98692, 98776, 98948, 99032, 99204, 99288, 99460, 99544, 99716, 99800,
            99972,
        ];

        let mut seed = 0x1f23_ab13;
        static mut SRC: Vec<u8> = Vec::new();
        for v in 0..100_000 {
            seed ^= v;
            unsafe {
                SRC.push((seed & 0xff) as u8);
            }
        }
        let chunk_offsets = Config::BuzHash(FilterConfig {
            filter_bits: FilterBits(6),
            min_chunk_size: 64,
            max_chunk_size: 1024,
            window_size: 20,
        })
        .new_chunker(&mut Box::new(unsafe { &SRC[..] }))
        .map(|result| {
            let (offset, _chunk) = result.unwrap();
            offset
        })
        .collect::<Vec<u64>>()
        .await;
        assert_eq!(chunk_offsets, expected_chunk_offsets);
    }
}
