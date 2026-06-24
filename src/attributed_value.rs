use std::path::PathBuf;

pub(crate) enum AttributedValue<T> {
    FromEnvironmentVariable {
        value: T,
        environment_variable_name: &'static str,
    },
    FromSettingsFile {
        value: T,
        settings_file_path: PathBuf,
        profile: Option<String>,
    },
    FromKeychain {
        value: T,
        entry_name: String,
    },
}

impl<T> AttributedValue<T> {
    pub(crate) fn unwrap(&self) -> &T {
        match self {
            Self::FromEnvironmentVariable { value, .. } => value,
            Self::FromSettingsFile { value, .. } => value,
            Self::FromKeychain { value, .. } => value,
        }
    }
}
