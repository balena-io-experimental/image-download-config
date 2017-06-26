'use strict';

const Promise = require('bluebird');
const aws = require('aws-sdk');
const filedisk = Promise.promisifyAll(require('file-disk'));
const fs = require('fs');
const imagefs = require('resin-image-fs');
const partitioninfo = require('partitioninfo');
const progress = require('stream-progressbar');
const reconfix = require('reconfix');

const schema = require('./schema.json');
const config = require('./config.json');

let s3 = new aws.S3({
	accessKeyId: null,
	secretAccessKey: null,
	s3ForcePathStyle: true,
	sslEnabled: false
})

// Make it work without accessKeyId and secretAccessKey
for (let key of [ 'getObject', 'headObject' ]) {
	s3[key] = (...args) => {
		return s3.makeUnauthenticatedRequest(key, ...args);
	}
}

s3 = Promise.promisifyAll(s3);

const disk = new filedisk.S3Disk(
	s3,
	'resin-staging-img',
	'images/raspberry-pi/2.0.6+rev3.prod/image/resin.img',
	true,  // record reads
	true   // discard is zero
);

const outputFilename = 'out';
const blockSize = 512;

reconfix.writeConfiguration(schema, config, disk)
.then(() => {
	console.log('configured')
	return partitioninfo.getPartitions(disk)
})
.then((partitions) => {
	console.log('got partitions', partitions)
	let partitionNumbers = [];
	let partitionsByNumber = {};
	for (let i = 0; i < partitions.length; i++) {
		if (partitions[i].type === 131) {
			partitionNumbers.push(i + 1);
			partitionsByNumber[i + 1] = partitions[i];
		}
	}
	console.log('partitions to trim', partitionNumbers)
	const disposers = partitionNumbers.map((number) => {
		return imagefs.interact(disk, number)
		.catch((err) => {
			const part = partitionsByNumber[number];
			console.log('mount failed, discarding', part.size, 'bytes at', part.offset)
			return disk.discardAsync(part.offset, part.size)
			.return(null);
		})
	})
	return Promise.using(disposers, (filesystems) => {
		console.log('filsystems mounted')
		return Promise.map(filesystems, (fs) => {
			if (fs !== null) {
				fs = Promise.promisifyAll(fs);
				if (fs.trimAsync) {
					return fs.trimAsync();
				}
			}
		});
	});
})
.then(() => {
	console.log('filesystems trimmed')
	return disk.getCapacityAsync();
})
.then((size) => {
	console.log('got size', size)
	return disk.getStreamAsync(null, null, 16 * 1024 * 1024)
	.then((stream) => {
		console.log('got stream')
		const output = fs.createWriteStream(outputFilename);
		stream.pipe(progress('[:bar] :percent :etas', { total: size })).pipe(output);
		return new Promise((resolve, reject) => {
			stream.on('end', resolve);
			stream.on('error', reject);
		});
	});
})
.then(() => {
	console.log(disk.knownChunks.length, 'known chunks')
	const knownLength = disk.knownChunks.map((chunk) => {
		return Math.max(chunk.end - chunk.start + 1, 0)
	}).reduce((a, b) => {
		return a + b;
	});
	console.log('known chunks length:', knownLength, '/', disk.capacity)
//	console.log('knownChunks', disk.knownChunks.slice(0, 20))
	return disk.getBlockMapAsync(blockSize);
})
.then((bmap) => {
	console.log('blockmap:', bmap.toString());
	const bytesToWrite = bmap.ranges.map((range) => {
		return (range.end - range.start + 1) * blockSize
	}).reduce((a, b) => {
		return a + b
	})
	console.log('bytesToWrite', bytesToWrite)
	// Read what we've downloaded to be sure.
	Promise.using(filedisk.openFile(outputFilename, 'r'), (fd) => {
		const disk2 = new filedisk.FileDisk(fd, true, true);
		return Promise.using(imagefs.interact(disk2, 1), (filesystem) => {
			filesystem = Promise.promisifyAll(filesystem, { multiArgs: true });
			return filesystem.openAsync('CONFIG.TXT', 'r')
			.then((fd) => {
				const buf = Buffer.allocUnsafe(1024)
				return filesystem.readAsync(fd, buf, 0, 1024, 0)
			})
			.spread((len, buf) => {
				console.log('data in config.json in the first partition:', buf.slice(0, len).toString('utf8'), len);
			})
		});
	});
});
