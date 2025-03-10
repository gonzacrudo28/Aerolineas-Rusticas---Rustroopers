use egui::ColorImage;
use egui::Context;
use walkers::sources::Attribution;
use walkers::Texture;
use walkers::TileId;
use walkers::Tiles;

/// Struct representing local tiles for rendering.
pub struct LocalTiles {
    egui_ctx: Context,
}

impl LocalTiles {
    /// Creates a new instance of `LocalTiles`.
    ///
    /// # Arguments
    /// * `egui_ctx` - The Egui context.
    ///
    /// # Returns
    /// `LocalTiles` - A new instance of `LocalTiles`.
    pub fn _new(egui_ctx: Context) -> Self {
        Self { egui_ctx }
    }
}

impl Tiles for LocalTiles {
    /// Retrieves the texture for a given tile ID.
    ///
    /// # Arguments
    /// * `_tile_id` - The ID of the tile.
    ///
    /// # Returns
    /// `Option<Texture>` - The texture for the given tile ID, or `None` if not available.
    fn at(&mut self, _tile_id: TileId) -> Option<Texture> {
        let image = ColorImage::new([256, 256], egui::Color32::WHITE);

        Some(Texture::from_color_image(image, &self.egui_ctx))
    }

    /// Provides the attribution information for the tiles.
    ///
    /// # Returns
    /// `Attribution` - The attribution information.
    fn attribution(&self) -> Attribution {
        Attribution {
            text: "Local rendering example",
            url: "https://github.com/podusowski/walkers",
            logo_light: None,
            logo_dark: None,
        }
    }

    /// Returns the size of the tiles.
    ///
    /// # Returns
    /// `u32` - The size of the tiles.
    fn tile_size(&self) -> u32 {
        256
    }
}
