use std::path::PathBuf;

pub(crate) enum AttributedValue<T> {
    EnvironmentVariable {
        value: T,
        environment_variable_name: &'static str,
    },
    SettingsFile {
        value: T,
        settings_file_path: PathBuf,
        profile: Option<String>,
    },
    Keychain {
        value: T,
        entry_name: String,
    },
}

impl<T> AttributedValue<T> {
    pub(crate) fn unwrap(&self) -> &T {
        match self {
            Self::EnvironmentVariable { value, .. } => value,
            Self::SettingsFile { value, .. } => value,
            Self::Keychain { value, .. } => value,
        }
    }
}
