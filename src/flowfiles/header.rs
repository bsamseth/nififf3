use std::collections::HashMap;

/// Representation of the header of a NiFi Flow File v3.
///
/// A NiFi Flow File v3 header contains, when decoded, all the attributes attached to the content,
/// as well as the size in bytes of the content.
#[derive(Debug, Clone)]
pub struct FlowFileHeader {
    size: u64,
    attributes: HashMap<String, String>,
}

impl FlowFileHeader {
    /// Create a new flow file header.
    ///
    /// The size is the number of bytes in the content of the flow file, not including the
    /// size of this header itself.
    #[must_use]
    pub fn new(size: u64, attributes: HashMap<String, String>) -> Self {
        Self { size, attributes }
    }

    /// The length of the content of the flow file this header describes.
    ///
    /// Note that this is not how many bytes may be left in the content,
    /// but rather how many bytes the content is expected to contain in total, according
    /// to the flow file header.
    #[must_use]
    pub fn len(&self) -> u64 {
        self.size
    }

    /// Return `true` if the flow file self-reports to be empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// All attributes contained in the flow file.
    #[must_use]
    pub fn attributes(&self) -> &HashMap<String, String> {
        &self.attributes
    }

    /// All attributes contained in the flow file.
    #[must_use]
    pub fn attributes_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.attributes
    }
}
