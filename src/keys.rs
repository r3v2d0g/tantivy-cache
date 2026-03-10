use std::path::Path;

use eyre::{OptionExt, Result};

/// Generates the key that should be used to store the footer for the given path.
pub fn footer(path: &Path) -> Result<String> {
    let path = path.to_str().ok_or_eyre("invalid path")?;
    Ok(format!("footer:{path}"))
}
