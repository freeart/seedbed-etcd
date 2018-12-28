const assert = require('assert'),
	async = require('async'),
	fs = require('fs'),
	path = require('path'),
	configStorage = require('etcd-fb')

module.exports = function () {
	assert(!this.etcd, "field exists")

	const isObject = val =>
		typeof val === 'object' && !Array.isArray(val);

	const keys = async.queue(function (task, cb) {
		configStorage.set(`/${task.path.join("/")}${task.value ? "" : "/"}`, task.value, null, cb)
	}, 1);

	keys.drain = function () {
		console.log('all items have been processed');
	};

	function inspect(obj, stack = []) {
		if (isObject(obj)) {
			Object.entries(obj).forEach(([key, value]) => {
				inspect(value, stack.concat(key))
			})
		} else {
			keys.push({ path: stack, value: obj })
		}
	}

	this.etcd = () => {
		configStorage.config(process.env.ETCD || "http://127.0.0.1:2379");
		configStorage.__connect();

		const filename = path.join(__dirname, `../../etcd-${process.env.NODE_ENV}.json`);
		const backup = path.join(__dirname, `../../etcd-${process.env.NODE_ENV}.json.bak`);

		if (!fs.existsSync(filename)) {
			throw new Error(`${filename} not found`)
		}
		async.series([
			(cb) => {
				configStorage.conn.get(`/${process.env.NODE_ENV}/`, { recursive: true, maxRetries: 100 }, (err, val) => {
					if (err) {
						if (err.message == "Key not found") {
							return cb()
						}
						return cb(err)
					}
					const data = configStorage.__convert(`/${process.env.NODE_ENV}/`, val.node.nodes)
					fs.writeFile(backup, JSON.stringify({ [process.env.NODE_ENV]: data }, null, 4), 'utf8', cb);
				})
			},
			(cb) => configStorage.conn.del(`/${process.env.NODE_ENV}/`, { recursive: true, dir: true, maxRetries: 100 }, (err) => {
				if (err) {
					if (err.message == "Key not found") {
						return cb()
					}
					return cb(err)
				}
				cb()
			})
		], (err) => {
			if (err) {
				throw new Error(err)
			}
			console.log(`created ${backup}`);
			const schema = require(filename);
			inspect(schema)
		})
	};

	this.etcd();

	return Promise.resolve();
}