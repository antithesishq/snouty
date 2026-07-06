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
    pub(crate) fn unwrap(&self) -> &T {
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

    pub(crate) fn map<O, F>(self, func: F) -> AttributedValue<O>
    where
        F: Fn(T) -> O,
    {
        match self {
            Self::EnvironmentVariable {
                value,
                environment_variable_names,
            } => AttributedValue::EnvironmentVariable {
                value: func(value),
                environment_variable_names,
            },
            Self::SettingsFile {
                value,
                settings_file_path,
                profile,
            } => AttributedValue::SettingsFile {
                value: func(value),
                settings_file_path,
                profile,
            },
            Self::Keychain { value, entry_name } => AttributedValue::Keychain {
                value: func(value),
                entry_name,
            },
        }
    }
}
