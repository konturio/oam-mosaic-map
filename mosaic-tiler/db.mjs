import pg from "pg";

const pool = new pg.Pool({
  poolSize: parseInt(process.env.DB_POOL_SIZE) || 16,
  ssl: {
    rejectUnauthorized: false,
  },
});

function getWaitingCount() {
  return pool.waitingCount;
}

async function query(text, params) {
  const start = Date.now();
  const res = await pool.query(text, params);
  const duration = Date.now() - start;
  if (duration > 5000) {
    console.log("long db query", {
      text: text,
      params: params,
      duration,
      rows: res.rowCount,
    });
  }
  return res;
}

async function getClient() {
  const client = await pool.connect();
  const query = client.query;
  const release = client.release;
  // set a timeout of 5 seconds, after which we will log this client's last query
  const timeout = setTimeout(() => {
    console.error("A client has been checked out for more than 5 seconds!");
    console.error(
      `The last executed query on this client was: ${client.lastQuery}`
    );
  }, 5000);
  // monkey patch the query method to keep track of the last query executed
  client.query = (...args) => {
    client.lastQuery = args;
    return query.apply(client, args);
  };
  client.release = () => {
    // clear our timeout
    clearTimeout(timeout);
    // set the methods back to their old un-monkey-patched version
    client.query = query;
    client.release = release;
    return release.apply(client);
  };
  return client;
}

export { query, getClient, getWaitingCount };
