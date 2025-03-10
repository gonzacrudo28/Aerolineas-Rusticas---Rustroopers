#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// Enum representing different map providers.
pub enum Provider {
    OpenStreetMap,
    MapboxStreets,
    MapboxSatellite,
}
