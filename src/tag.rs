//! Tagged values: a value paired with a caller-defined tag.

/// A value carrying a tag.
#[derive(Debug)]
pub struct Tagged<V, T> {
    value: V,
    tag: T,
}

impl<V, T> Tagged<V, T> {
    pub fn new(value: V, tag: T) -> Self {
        Self { value, tag }
    }

    pub fn value(&self) -> &V {
        &self.value
    }

    pub fn tag(&self) -> &T {
        &self.tag
    }

    /// The inner value, dropping the tag.
    pub fn untag(self) -> V {
        self.value
    }
}

/// Wrap any value in a [`Tagged`].
pub trait Tag: Sized {
    fn with_tag<T>(self, tag: T) -> Tagged<Self, T> {
        Tagged::new(self, tag)
    }
}

impl<V> Tag for V {}
