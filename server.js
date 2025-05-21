'use strict';
const express = require('express');
const duckdb = require('duckdb');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const app = express();
const port = 3002;

/**
 * Data sources are stored on EDITO at
 * 
 * https://minio.dive.edito.eu/project-plastic-marine-debris-drift/RUN_2010_5YEARS/Trajectories_trajectories_smoc_2010-1-1_1825days_coastalrepel.parquet
 */
const GEOPARQUET_FILES = getGeoparquets();

// Caching mechanism
const useCache = false;
const CACHE_DIR = '/cache';
if (!fs.existsSync(CACHE_DIR)) {
    fs.mkdirSync(CACHE_DIR);
}

// Connect to DuckDB
const db = new duckdb.Database("/data/duckdb.db");

console.log('Loading duckdb extensions...');
db.exec("INSTALL spatial; LOAD spatial;");
console.log('...done !');

// to support JSON-encoded bodies
app.use(express.json());

// to support URL-encoded bodies
app.use(express.urlencoded({ extended: true }));

app.use(function (req, res, next) {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
    next();
});

app.get("/", async (req, res) => {
    res.status(200).json({ "message": "hello" });
});

// API
/*app.get('/search', getOrigin);
app.get('/origin', getOrigin);
app.get('/tracks', getTracks)
app.get('/max', getMax);
app.get('/maxInTime', getMaxInTime);*/

// Start the server
app.listen(port, () => {
    console.log(`Server running at http://localhost:${port}`);
});

/** 
 * DATA STRUCTURE
 */
const columns = [
    "p.trajectory",
    "p.obs",
    "p.CI",
    "p.EEZ",
    "p.MPW",
    "p.RI",
    "p.RI_annual",
    "p.age",
    "p.distcoast",
    "p.time",
    "p.travelled_distance",
    "p.z",
    "ST_AsGeoJSON(p.geometry) as geometry"
];

/**
 * Get tracks
 */
app.get("/tracks", async (req, res) => {

    const queryParams = parseQueryParams(req.query);
    const limit = queryParams.limit || 100000;

    const queryString = `
        SELECT ${columns.join(',')} FROM '${GEOPARQUET_FILE}' p LIMIT ${limit}
    `;
    launchQuery(queryString, queryParams, GEOPARQUET_FILE, res);

});


/**
 * Get track by id
 */
app.get("/tracks/:trackId", async (req, res) => {

    const queryParams = parseQueryParams(req.query);
    const queryString = `
        SELECT ${columns.join(',')} FROM '${GEOPARQUET_FILE}' p
        WHERE trajectory = ${req.params.trackId}
    `;
    launchQuery(queryString, queryParams, GEOPARQUET_FILE, res);

});


/**
 * Get destination from area
 */
app.get("/destination", async (req, res) => {

    const queryParams = parseQueryParams(req.query);
    if (!queryParams.intersects) {
        return res.status(400).json({
            error: 'Mandatory bbox or intersects is missing'
        });
    }

    if (!queryParams.timeRange) {
        return res.status(400).json({
            error: 'Mandatory datetime is missing'
        });
    }

    function queryString(parquetFile) {
        return `
            WITH cte AS (
                SELECT trajectory FROM '${parquetFile}'
                WHERE obs = 0
                AND ST_Intersects(geometry, ST_GeomFromText('${queryParams.intersects}'))
            )
            SELECT ${columns.join(',')} FROM '${parquetFile}' p
            JOIN cte c
            ON c.trajectory = p.trajectory
            ORDER by p.trajectory, p.obs
        `;
    };

    let queries = [];

    if (queryParams.timeRange) {
        if ( !GEOPARQUET_FILES[queryParams.timeRange[0]] ) {
            return res.status(400).json({
                error: 'No data associated with datetime'
            });
        }
        else {
            queries.push(runQuery(queryString(GEOPARQUET_FILES[queryParams.timeRange[0]])));
        }
    }
    else {
        for (var key in GEOPARQUET_FILES) {
            queries.push(runQuery(queryString(GEOPARQUET_FILES[key])));
        }
    }

    var hrstart = process.hrtime()

    try {
        const results = await Promise.all(queries);

        let geojson = {
            type: 'FeatureCollection',
            context: {
                query: {
                    processingTime: process.hrtime(hrstart)
                }
            },
            features:[]
        };

        for (var i = 0, ii = results.length; i < ii; i++) {
            for (var j = 0, jj = results[i].length; j < jj; j++) {
                let properties = {};
                let row = results[i][j];
                let isoTime = results[i][0].time.toISOString().substring(0,10);
                for (var key in row) {
                    if (['geometry'].includes(key)) {
                        continue;
                    }
                    properties[key] = typeof row[key] === 'bigint' ? row[key].toString() : row[key];
                }
                properties['trajectory'] = [isoTime, properties['trajectory']].join('_');
                geojson.features.push({
                    type: 'Feature',
                    id: [isoTime, row.trajectory, row.obs].join('_'),
                    properties: properties,
                    geometry: JSON.parse(row.geometry), // Parsing GeoJSON geometry
                });
            }
        }

        // Return the GeoJSON response
        res.setHeader('Content-Type', 'application/geo+json');
        res.json(geojson);
        
    } catch (e) {
        setImmediate(() => {
            return res.status(500).json({
                error: e.message,
                stack: e.stack
            });
        });
    }

});

/**
 * Get destination from area
 */
app.get("/origin", async (req, res) => {

    const queryParams = parseQueryParams(req.query);
    if (!queryParams.intersects) {
        return res.status(400).json({
            error: 'Mandatory bbox or intersects is missing'
        });
    }

    if (!queryParams.timeRange) {
        return res.status(400).json({
            error: 'Mandatory datetime is missing'
        });
    }

    function queryString(parquetFile) {
        return `
            WITH cte AS (
                SELECT DISTINCT trajectory FROM '${parquetFile}'
                WHERE ST_Intersects_Extent(geometry, ST_GeomFromText('${queryParams.intersects}'))
            )
            SELECT ${columns.join(',')} FROM '${parquetFile}' p
            JOIN cte c
            ON c.trajectory = p.trajectory
            WHERE p.obs = 0
            ORDER by p.trajectory, p.obs
        `;
    };

    let queries = [];
    
    if (queryParams.timeRange) {
        if ( !GEOPARQUET_FILES[queryParams.timeRange[0]] ) {
            return res.status(400).json({
                error: 'No data associated with datetime'
            });
        }
        else {
            queries.push(runQuery(queryString(GEOPARQUET_FILES[queryParams.timeRange[0]])));
        }
    }
    else {
        for (var key in GEOPARQUET_FILES) {
            queries.push(runQuery(queryString(GEOPARQUET_FILES[key])));
        }
    }

    var hrstart = process.hrtime()

    try {
        const results = await Promise.all(queries);

        let geojson = {
            type: 'FeatureCollection',
            context: {
                query: {
                    processingTime: process.hrtime(hrstart)
                }
            },
            features:[]
        };

        for (var i = 0, ii = results.length; i < ii; i++) {
            for (var j = 0, jj = results[i].length; j < jj; j++) {
                let properties = {};
                let row = results[i][j];
                let isoTime = results[i][0].time.toISOString().substring(0,10);
                for (var key in row) {
                    if (['geometry'].includes(key)) {
                        continue;
                    }
                    properties[key] = typeof row[key] === 'bigint' ? row[key].toString() : row[key];
                }
                properties['trajectory'] = [isoTime, properties['trajectory']].join('_');
                geojson.features.push({
                    type: 'Feature',
                    id: [isoTime, row.trajectory, row.obs].join('_'),
                    properties: properties,
                    geometry: JSON.parse(row.geometry), // Parsing GeoJSON geometry
                });
            }
        }

        // Return the GeoJSON response
        res.setHeader('Content-Type', 'application/geo+json');
        res.json(geojson);
        
    } catch (e) {
        setImmediate(() => {
            return res.status(500).json({
                error: e.message,
                stack: e.stack
            });
        });
    }

});


/** =================================================================================== */

function getGeoparquets() {

    const rootUrl = 'https://minio.dive.edito.eu/project-plastic-marine-debris-drift/';
    
    var geoparquets = [];

    var years = [2010];
    var months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
    var days = [1, 8, 15, 22];

    for (var i = 0, ii = years.length; i < ii; i++) {
        for (var j = 0, jj = months.length; j < jj; j++) {
            for (var k = 0, kk = days.length; k < kk; k++) {
                var id = [[years[i], months[j].toString().padStart(2, '0'), days[k].toString().padStart(2, '0')].join('-')];
                geoparquets[id] = rootUrl + 'RUN_' + years[i] + '_5YEARS_GEOPARQUET/Trajectories_smoc_' + [years[i], months[j], days[k]].join('-') + '_1825days_coastalrepel.parquet';
                //geoparquets[id] = '/data/Trajectories_smoc_' + [years[i], months[j], days[k]].join('-') + '_1825days_coastalrepel.parquet';
            }
        }
    }

    return geoparquets;
}

/*
 * Parse input query parameters common to 
 * all API endpoints
 * 
 * Common query parameters are :
 * 
 *   - "limit"      : an integer greater than 0
 *   - "bbox"       : format lonMin,latMin,lonMax,latMax
 *   - "intersects" : WKT polygon string
 *   - "datetime"   : ISO8601 range i.e. YYYY-MM-DDTHH:MM:SS(Z)/YYYY-MM-DDTHH:MM:SS(Z)
 * 
 */
function parseQueryParams(query) {

    var inputs = {};

    // Limit
    if (query.hasOwnProperty('limit')) {
        if (! /^\d+$/.test(query.limit)) {
            return res.status(400).json({
                error: 'Invalid limit. Should be an integer greater than 0',
            });
        }
        inputs.limit = parseInt(query.limit);
    }

    // BBOX is lonMin,latMin,lonMax,latMax
    if (query.hasOwnProperty('bbox')) {
        var coords = query.bbox.split(',');
        inputs.intersects = 'POLYGON((' + coords[0] + ' ' + coords[1] + ',' + coords[0] + ' ' + coords[3] + ',' + coords[2] + ' ' + coords[3] + ',' + coords[2] + ' ' + coords[1] + ',' + coords[0] + ' ' + coords[1] + '))';
    }

    // Intersects
    if (query.hasOwnProperty('intersects')) {
        inputs.intersects = query.intersects;
    }

    // Datetime
    if (query.hasOwnProperty('datetime')) {
        inputs.timeRange = query.datetime.split('/');
    }

    // Traj
    if (query.hasOwnProperty('traj')) {
        inputs.traj = parseBool(query.traj);
    }

    // OrderBy
    if (query.hasOwnProperty('orderBy')) {
        inputs.orderBy = query.orderBy;
    }

    // Minimalist output (for getMaxInTime)
    if (query.hasOwnProperty('minimalist')) {
        inputs.minimalist = parseBool(query.minimalist);
    }

    return inputs;

}

function parseBool(str) {
    return !(
        str === 'false' ||
        str === '0' ||
        str === '' ||
        str === undefined
    );
};

// Helper function to run a query and return a promise
function runQuery(queryString) {

    return new Promise((resolve, reject) => {
        db.all(queryString, (err, rows) => {
            if (err) {
                return reject(err);
            }
            resolve(rows);
        });
    });

}

function launchQuery(queryString, queryParams, geoparquetFile, res) {

    if (!checkDataExist(geoparquetFile)) {
        res.status(400).json({ error: "Parquet file is not available " + geoparquetFile });
    }

    var hrstart = process.hrtime()

    try {

        db.all(queryString, (err, rows) => {

            if (err) {
                throw err;
            }

            // Construct GeoJSON FeatureCollection
            const geojson = {
                type: 'FeatureCollection',
                links: [
                    {
                        href: geoparquetFile,
                        rel: 'data',
                        title: 'GeoParquet file',
                        type: 'application/vnd.apache.parquet'
                    }
                ],
                context: {
                    returned: rows.length,
                    limit: queryParams.hasOwnProperty('limit') ? queryParams.limit : -1,
                    query: {
                        processingTime: process.hrtime(hrstart)
                    }
                },
                features: rows.map(row => {
                    let properties = {};
                    for (var key in row) {
                        if (['geometry'].includes(key)) {
                            continue;
                        }
                        properties[key] = typeof row[key] === 'bigint' ? row[key].toString() : row[key];
                    }
                    return {
                        type: 'Feature',
                        id: [row.trajectory, row.obs].join('_'),
                        properties: properties,
                        geometry: JSON.parse(row.geometry), // Parsing GeoJSON geometry
                    };
                })
            };

            // Return the GeoJSON response
            res.setHeader('Content-Type', 'application/geo+json');
            res.json(geojson);
        });

    } catch (error) {
        setImmediate(() => {
            return res.status(500).json({
                error: e.message,
                stack: e.stack
            });
        });
    }

}

/**
 * Hash a query to be used as a cache key
 * 
 * @param {string} query 
 * @returns 
 */
function hashQuery(query) {
    return crypto.createHash('sha256').update(query).digest('hex');
}

/**
 * Get cached result
 * 
 * @param {string} hash 
 * @returns 
 */
function getCachedResult(hash) {
    const cacheFile = path.join(CACHE_DIR, `${hash}.json`);
    if (fs.existsSync(cacheFile)) {
        return JSON.parse(fs.readFileSync(cacheFile, 'utf8'));
    }
    return null;
};

/**
 * Save data to cache
 * 
 * @param {string} hash 
 * @param {string} data 
 */
function saveToCache(hash, data) {
    const cacheFile = path.join(CACHE_DIR, `${hash}.json`);
    fs.writeFileSync(cacheFile, JSON.stringify(data), 'utf8');
};

async function checkDataExist(geoparquetFile) {
    if (geoparquetFile.startsWith('http')) {
        if (! await urlExist(geoparquetFile)) {
            return false;
        }
    }
    else if (!checkFileExistsSync(geoparquetFile)) {
        return false;
    }
    return true;
}

async function urlExist(url) {
    try {
        const response = await fetch(url, { method: 'HEAD' });
        return response.status === 200 ? true : false;
    }
    catch (error) {
        return false;
    }
}

function checkFileExistsSync(filepath) {
    let flag = true;
    try {
        fs.accessSync(filepath, fs.constants.F_OK);
    } catch (e) {
        flag = false;
    }
    return flag;
}

