pub enum Error {
    NotAnArchive(String),
    ChecksumMismatch(String),
    IO(String, std::io::Error),
    DictionaryDecode(String, prost::DecodeError),
    DictionaryEncode(String, prost::EncodeError),
    #[cfg(feature = "lzma-compression")]
    LZMA(String, lzma::LzmaError),
    Hyper(String, hyper::Error),
    Http(String, hyper::http::Error),
    InvalidUri(String, hyper::http::uri::InvalidUri),
    JoinError(String, tokio::task::JoinError),
    Other(String),
    Wrapped(String, Box<Error>),
}

impl Error {
    pub fn wrap(self, desc: &str) -> Self {
        Error::Wrapped(desc.to_owned(), Box::new(self))
    }
}
#[cfg(feature = "lzma-compression")]
impl From<(&str, lzma::LzmaError)> for Error {
    fn from((desc, e): (&str, lzma::LzmaError)) -> Self {
        Error::LZMA(desc.to_owned(), e)
    }
}

impl From<(&str, prost::DecodeError)> for Error {
    fn from((desc, e): (&str, prost::DecodeError)) -> Self {
        Error::DictionaryDecode(desc.to_owned(), e)
    }
}

impl From<(&str, prost::EncodeError)> for Error {
    fn from((desc, e): (&str, prost::EncodeError)) -> Self {
        Error::DictionaryEncode(desc.to_owned(), e)
    }
}

impl From<(&str, std::io::Error)> for Error {
    fn from((desc, e): (&str, std::io::Error)) -> Self {
        Error::IO(desc.to_owned(), e)
    }
}

impl From<(String, std::io::Error)> for Error {
    fn from((desc, e): (String, std::io::Error)) -> Self {
        Error::IO(desc, e)
    }
}

impl From<(&str, hyper::Error)> for Error {
    fn from((desc, e): (&str, hyper::Error)) -> Self {
        Error::Hyper(desc.to_owned(), e)
    }
}

impl From<(&str, hyper::http::Error)> for Error {
    fn from((desc, e): (&str, hyper::http::Error)) -> Self {
        Error::Http(desc.to_owned(), e)
    }
}

impl From<(&str, hyper::http::uri::InvalidUri)> for Error {
    fn from((desc, e): (&str, hyper::http::uri::InvalidUri)) -> Self {
        Error::InvalidUri(desc.to_owned(), e)
    }
}

impl From<(&str, tokio::task::JoinError)> for Error {
    fn from((desc, e): (&str, tokio::task::JoinError)) -> Self {
        Error::JoinError(desc.to_owned(), e)
    }
}

impl From<&str> for Error {
    fn from(desc: &str) -> Self {
        Error::Other(desc.to_owned())
    }
}

impl From<String> for Error {
    fn from(desc: String) -> Self {
        Error::Other(desc)
    }
}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NotAnArchive(desc) => write!(f, "{}", desc),
            Error::ChecksumMismatch(desc) => write!(f, "{}", desc),
            Error::IO(desc, e) => write!(f, "{}: {:?}", desc, e),
            Error::DictionaryEncode(desc, e) => write!(f, "{}: {:?}", desc, e),
            Error::DictionaryDecode(desc, e) => write!(f, "{}: {:?}", desc, e),
            #[cfg(feature = "lzma-compression")]
            Error::LZMA(desc, e) => write!(f, "{}: {:?}", desc, e),
            Error::Hyper(desc, e) => write!(f, "{}: {:?}", desc, e),
            Error::Http(desc, e) => write!(f, "{}: {:?}", desc, e),
            Error::InvalidUri(desc, e) => write!(f, "{}: {:?}", desc, e),
            Error::JoinError(desc, e) => write!(f, "{}: {:?}", desc, e),
            Error::Other(desc) => write!(f, "{}", desc),
            Error::Wrapped(desc, e) => write!(f, "{}: {:?}", desc, e),
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NotAnArchive(ref desc) => write!(f, "{}", desc),
            Error::ChecksumMismatch(ref desc) => write!(f, "{}", desc),
            Error::IO(ref desc, ref e) => write!(f, "{}: {}", desc, e),
            Error::DictionaryEncode(ref desc, ref e) => write!(f, "{}: {}", desc, e),
            Error::DictionaryDecode(ref desc, ref e) => write!(f, "{}: {}", desc, e),
            #[cfg(feature = "lzma-compression")]
            Error::LZMA(ref desc, ref e) => write!(f, "{}: {}", desc, e),
            Error::Hyper(ref desc, ref e) => write!(f, "{}: {}", desc, e),
            Error::Http(ref desc, ref e) => write!(f, "{}: {}", desc, e),
            Error::InvalidUri(desc, e) => write!(f, "{}: {}", desc, e),
            Error::JoinError(desc, e) => write!(f, "{}: {:?}", desc, e),
            Error::Other(ref desc) => write!(f, "{}", desc),
            Error::Wrapped(desc, e) => write!(f, "{}: {}", desc, e),
        }
    }
}

impl std::error::Error for Error {}
