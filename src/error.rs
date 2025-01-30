#[derive(Debug)]
pub struct Error {
    pub details: String,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.details)
    }
}

impl Error {
    pub fn new(msg: impl Into<String>) -> Error {
        Error {
            details: msg.into(),
        }
    }
}

impl std::error::Error for Error {
    fn description(&self) -> &str {
        &self.details
    }
}
