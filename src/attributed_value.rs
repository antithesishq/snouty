use std::path::PathBuf;

pub(crate) enum AttributedValue<T> {
    EnvironmentVariable {
        value: T,
        environment_variable_names: Vec<&'static str>,
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
    pub(crate) fn value(&self) -> &T {
        match self {
            Self::EnvironmentVariable { value, .. } => value,
            Self::SettingsFile { value, .. } => value,
            Self::Keychain { value, .. } => value,
        }
    }

    pub(crate) fn extract(self) -> T {
        match self {
            Self::EnvironmentVariable { value, .. } => value,
            Self::SettingsFile { value, .. } => value,
            Self::Keychain { value, .. } => value,
        }
    }

    pub(crate) fn with_value<TValue>(self, value: TValue) -> AttributedValue<TValue> {
        match self {
            Self::EnvironmentVariable {
                environment_variable_names,
                ..
            } => AttributedValue::EnvironmentVariable {
                value,
                environment_variable_names,
            },
            Self::Keychain { entry_name, .. } => AttributedValue::Keychain { value, entry_name },
            Self::SettingsFile {
                settings_file_path,
                profile,
                ..
            } => AttributedValue::SettingsFile {
                value,
                settings_file_path,
                profile,
            },
        }
    }
}
