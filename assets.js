
let assetsMetadata = {};

function getAssetID(asset) {
	for (let id in assetsMetadata) {
		if (id === asset || assetsMetadata[id].name === asset) return id;
	}
	return asset;
}


exports.assetsMetadata = assetsMetadata;
exports.getAssetID = getAssetID;
