
let assetsMetadata = {};

function getAssetID(asset) {
	for (let id in assetsMetadata) {
		if (id === asset || assetsMetadata[id].name === asset) return id;
	}
	return asset;
}

function getAssetName(asset) {
	for (let id in assetsMetadata) {
		if (id === asset) return assetsMetadata[id].name || asset;
	}
	return asset;
}

exports.assetsMetadata = assetsMetadata;
exports.getAssetID = getAssetID;
exports.getAssetName = getAssetName;
