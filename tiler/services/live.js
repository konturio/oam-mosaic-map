const config = require('../utils/config-helper').getConfig();
const pg = require('pg'),
    pool = new pg.Pool(config.pg);

module.exports.isLive = async function isLive() {
    const client = await pool.connect();
    let res;
    try {
        res = await client.query("select true");
    } catch (err) {
        throw err;
    }
    finally {
        client.release();
    }
    return res.rows;
};