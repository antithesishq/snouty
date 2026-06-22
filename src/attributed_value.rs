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
}

impl<T> AttributedValue<T> {
    pub(crate) fn unwrap(&self) -> &T {
        match self {
            Self::FromEnvironmentVariable {
                value,
                environment_variable_name: _,
            } => value,
            Self::FromSettingsFile {
                value,
                settings_file_path: _,
                profile: _,
            } => value,
        }
    }
}
