var gallPetersMapType;

function initGallPeters() {
    var GALL_PETERS_RANGE_X = 800;
    var GALL_PETERS_RANGE_Y = 512;

    // Fetch Gall-Peters tiles stored locally on our server.
    gallPetersMapType = new google.maps.ImageMapType({
        getTileUrl: function (coord, zoom) {
            var scale = 1 << zoom;

            // Wrap tiles horizontally.
            var x = ((coord.x % scale) + scale) % scale;

            // Don't wrap tiles vertically.
            var y = coord.y;
            if (y < 0 || y >= scale)
                return null;

            return 'https://developers.google.com/maps/documentation/' +
                'javascript/examples/full/images/gall-peters_' + zoom +
                '_' + x + '_' + y + '.png';
        },
        tileSize: new google.maps.Size(GALL_PETERS_RANGE_X, GALL_PETERS_RANGE_Y),
        isPng: true,
        minZoom: 0,
        maxZoom: 1,
        name: 'Gall-Peters'
    });

    // Describe the Gall-Peters projection used by these tiles.
    gallPetersMapType.projection = {
        fromLatLngToPoint: function (latLng) {
            var latRadians = latLng.lat() * Math.PI / 180;
            return new google.maps.Point(
                GALL_PETERS_RANGE_X * (0.5 + latLng.lng() / 360),
                GALL_PETERS_RANGE_Y * (0.5 - 0.5 * Math.sin(latRadians)));
        },
        fromPointToLatLng: function (point, noWrap) {
            var x = point.x / GALL_PETERS_RANGE_X;
            var y = Math.max(0, Math.min(1, point.y / GALL_PETERS_RANGE_Y));

            return new google.maps.LatLng(
                Math.asin(1 - 2 * y) * 180 / Math.PI, -180 + 360 * x,
                noWrap);
        }
    };
}