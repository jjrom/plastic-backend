'use strict';
const express = require('express');
const duckdb = require('duckdb');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const app = express();
const port = 3002;

const PLASTIC_CODES = {
    CI:0,
    RI:1
};

const continents = [
    {
        name: 'Europe',
        coordinates: [
            [
                [-10.898438, 36.315125], 
                [-10.898438, 44.024422],
                [-1.318359, 44.024422],
                [-1.318359, 36.315125],
                [-10.898438, 36.315125]
            ],
            [
                [11.777344, 34.633208], 
                [11.777344, 46.164614],
                [26.323242, 46.164614],
                [26.323242, 34.633208],
                [11.777344, 34.633208]
            ],
            [
                [11.777344, 34.633208], 
                [11.777344, 46.164614],
                [26.323242, 46.164614],
                [26.323242, 34.633208],
                [11.777344, 34.633208]
            ],
            [
                [-2.15332, 37.439974],
                [-2.15332, 46.709736],
                [11.865234, 46.709736],
                [11.865234, 37.439974],
                [-2.15332, 37.439974]
            ],
            [
                [-11.25, 43.325178],
                [-11.25, 59.355596],
                [27.070313, 59.355596],
                [27.070313, 43.325178],
                [-11.25, 43.325178]
            ]
        ]
    },
    {
        name: 'Africa',
        coordinates: [
            [
                [-0.263672, 32.916485],
                [-0.263672, 37.68382],
                [11.469727, 37.68382],
                [11.469727, 32.916485],
                [-0.263672, 32.916485]
            ],
            [
                [11.030273, 29.993002],
                [11.030273, 34.885931],
                [31.816406, 34.885931],
                [31.816406, 29.993002],
                [11.030273, 29.993002]
            ],
            [
                [-19.6875, -36.597889],
                [-19.6875, 30.902225],
                [60.46875, 30.902225],
                [60.46875, -36.597889],
                [-19.6875, -36.597889]
            ],
            [
                [-14.238281, 29.53523],
                [-14.238281, 36.031332],
                [11.425781, 36.031332],
                [11.425781, 29.53523],
                [-14.238281, 29.53523]
            ]
        ]
    },
    {
        name: 'Oceania',
        coordinates: [
            [
                [93.867188, -48.922499],
                [93.867188, 8.754795],
                [180, 8.754795],
                [180, -48.922499],
                [93.867188, -48.922499]
            ]
        ]
    },
    {
        name: 'America',
        coordinates: [
            [
                [-84.023438, -57.136239],
                [-84.023438, 15.961329],
                [-31.289063, 15.961329],
                [-31.289063, -57.136239],
                [-84.023438, -57.136239]
            ],
            [
                [-123.398438, 6.664608],
                [-123.398438, 30.751278],
                [-64.335938, 30.751278],
                [-64.335938, 6.664608],
                [-123.398438, 6.664608]
            ],
            [
                [-130.078125, 29.840644],
                [-130.078125, 52.48278],
                [-51.328125, 52.48278],
                [-51.328125, 29.840644],
                [-130.078125, 29.840644]
            ],
            [
                [-166.289063, 51.618017],
                [-166.289063, 73.528399],
                [-50.273438, 73.528399],
                [-50.273438, 51.618017],
                [-166.289063, 51.618017]
            ]
        ]
    },
    {
        name: 'Asia',
        coordinates: [
            [
                [47.109375, 18.979026],
                [47.109375, 32.546813],
                [123.75, 32.546813],
                [123.75, 18.979026],
                [47.109375, 18.979026]
            ],
            [
                [71.71875, 5.266008],
                [71.71875, 21.943046],
                [127.617188, 21.943046],
                [127.617188, 5.266008],
                [71.71875, 5.266008]
            ],
            [
                [115.664063, 18.979026],
                [115.664063, 62.431074],
                [147.304688, 62.431074],
                [147.304688, 18.979026],
                [115.664063, 18.979026]
            ]
        ]   
    }
];

/**
 * Data sources are stored on EDITO at
 * 
 * https://minio.dive.edito.eu/project-plastic-marine-debris-drift/RUN_2010_5YEARS/Trajectories_trajectories_smoc_2010-1-1_1825days_coastalrepel.parquet
 */
const GEOPARQUET_FILES = getGeoparquets();
const COASTLINES_FILE = '/data/coastlines.json';

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

// Ingest GeoJSON coastlines
/*console.log('Loading coastlines...');
db.exec("CREATE TABLE IF NOT EXISTS coastlines AS SELECT * FROM ST_Read('" + COASTLINES_FILE + "');");
db.exec(`
    CREATE TABLE coastlines AS
    WITH n1 AS (
        SELECT ST_DUMP(ST_POINTS(geom)) as geom FROM ST_Read('${COASTLINES_FILE}')
    ),
    n2 AS (
        SELECT UNNEST(geom, recursive := true) FROM n1
    )
    SELECT geom FROM n2;
`);

db.exec("CREATE INDEX IF NOT EXISTS geom_idx ON coastlines USING RTREE (geom);");
console.log('...done !');*/

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

    const queryParams = parseQueryParams(req.query, res);
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

    const queryParams = parseQueryParams(req.query, res);
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

    const queryParams = parseQueryParams(req.query, res);
    if (!queryParams.intersects) {
        return res.status(400).json({
            error: 'Mandatory bbox or intersects is missing'
        });
    }

    if (!queryParams.month) {
        return res.status(400).json({
            error: 'Mandatory month in the form "YYYY-MM" is missing'
        });
    }


    let queries = [];
    let keys = [queryParams.month];

    for (var i = 0, ii = keys.length; i < ii; i++) {
        if ( GEOPARQUET_FILES[keys[i]] && checkDataExist(GEOPARQUET_FILES[keys[i]])) {
            queries.push(runQuery(queryString(GEOPARQUET_FILES[keys[i]])));    
        }
    }

    if (queries.length === 0) {
        return res.status(400).json({
            error: 'No data associated with month ' + queryParams.month
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

    processQueryResult(queries, res);

});

/**
 * Get destination from area
 */
app.get("/origin", async (req, res) => {

    const queryParams = parseQueryParams(req.query, res);
    if (!queryParams.intersects) {
        return res.status(400).json({
            error: 'Mandatory bbox or intersects is missing'
        });
    }

    if (!queryParams.month) {
        return res.status(400).json({
            error: 'Mandatory month in the form "YYYY-MM" is missing'
        });
    }

    /*
     * Get the list of all parquet files 5 years before the month
     */
    let queries = [];
    let keys = [];
    let yyyy = parseInt(queryParams.month.substring(0,4));
    let mm = parseInt(queryParams.month.substring(5));
    for (var i = yyyy - 5; i <= yyyy; i++) {
        for (var j = 1; j <= 12; j++) {
            if ( (i === yyyy - 5 && j < mm) || (i === yyyy && j >= mm) ) {
                continue;
            }
            keys.push(i + '-' + j.toString().padStart(2, '0'));
        }
    }

    for (var i = 0, ii = keys.length; i < ii; i++) {
        if ( GEOPARQUET_FILES[keys[i]] && checkDataExist(GEOPARQUET_FILES[keys[i]])) {
            queries.push(runQuery(queryString(GEOPARQUET_FILES[keys[i]])));    
        }
    }

    if (queries.length === 0) {
        return res.status(400).json({
            error: 'No data associated with month ' + queryParams.month
        });
    }

    function queryString(parquetFile) {
        return `
            WITH cte AS (
                SELECT trajectory, time FROM '${parquetFile}'
                WHERE ST_Intersects_Extent(geometry, ST_GeomFromText('${queryParams.intersects}'))
                AND time BETWEEN '${queryParams.month + '-01'}' AND '${queryParams.month + '-28'}'
            )
            SELECT ${columns.join(',')}, c.time as time_hit FROM '${parquetFile}' p
            JOIN cte c
            ON c.trajectory = p.trajectory
            WHERE p.obs = 0
            ORDER by p.trajectory, p.obs
        `;
    };

    processQueryResult(queries, res);

});


/** =================================================================================== */

function getGeoparquets() {

    const rootUrl = 'https://minio.dive.edito.eu/project-plastic-marine-debris-drift/UNOC/';

    var geoparquets = [];

    var years = [2010];
    var months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];

    for (var i = 0, ii = years.length; i < ii; i++) {
        for (var j = 0, jj = months.length; j < jj; j++) {
            var id = [[years[i], months[j].toString().padStart(2, '0')].join('-')];
            geoparquets[id] = rootUrl + 'Trajectories_smoc_UNOC_' + id + '_1825days_coastalrepel.parquet';
            //geoparquets[id] = '/data/Trajectories_smoc_' + [years[i], months[j], days[k]].join('-') + '_1825days_coastalrepel.parquet';
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
function parseQueryParams(query, res) {

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
    if (query.hasOwnProperty('month')) {
        if (! /^\d{4}-\d{2}/.test(query.month)) {
            return res.status(400).json({
                error: 'Invalid month. Should be in the form YYYY-MM',
            });
        }
        inputs.month = query.month;
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

async function processQueryResult(queries, res) {

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
                let geometry = JSON.parse(row.geometry);
                for (var key in row) {
                    if (['geometry'].includes(key)) {
                        continue;
                    }
                    properties[key] = typeof row[key] === 'bigint' ? row[key].toString() : row[key];
                }
                properties['trajectory'] = [isoTime, properties['trajectory']].join('_');
                properties['locatedIn'] = getLocationName(geometry.coordinates);
                properties['plasticCode'] = row.RI_annual > 0.1 ? PLASTIC_CODES.RI: PLASTIC_CODES.CI;
                geojson.features.push({
                    type: 'Feature',
                    id: [isoTime, row.trajectory, row.obs].join('_'),
                    properties: properties,
                    geometry: geometry, // Parsing GeoJSON geometry
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
                    let geometry = JSON.parse(row.geometry);
                    for (var key in row) {
                        if (['geometry'].includes(key)) {
                            continue;
                        }
                        properties[key] = typeof row[key] === 'bigint' ? row[key].toString() : row[key];
                    }
                    properties['locatedIn'] = getLocationName(geometry.coordinates)
                    return {
                        type: 'Feature',
                        id: [row.trajectory, row.obs].join('_'),
                        properties: properties,
                        geometry: geometry // Parsing GeoJSON geometry
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

/**
 * Return location name from input point using PIP algorithm against continents
 * 
 * @param {Array} point 
 */
function getLocationName(point) {
    for (var i = continents.length; i--;) {
        for (var j = continents[i].coordinates.length; j--;) {    
            if (pointInPolygon(continents[i].coordinates[j], point)) {
                return continents[i].name;
            }
        }
    }
    return 'Unknown';
}

/**
 * Performs the even-odd-rule Algorithm (a raycasting algorithm) to find out whether a point is in a given polygon.
 * This runs in O(n) where n is the number of edges of the polygon.
 *
 * @param {Array} polygon an array representation of the polygon where polygon[i][0] is the x Value of the i-th point and polygon[i][1] is the y Value.
 * @param {Array} point   an array representation of the point where point[0] is its x Value and point[1] is its y Value
 * @return {boolean} whether the point is in the polygon (not on the edge, just turn < into <= and > into >= for that)
 */
function pointInPolygon(polygon, point) {

    //A point is in a polygon if a line from the point to infinity crosses the polygon an odd number of times
    let odd = false;
    
    //For each edge (In this case for each point of the polygon and the previous one)
    for (let i = 0, j = polygon.length - 1; i < polygon.length; i++) {

        //If a line from the point into infinity crosses this edge
        if (((polygon[i][1] > point[1]) !== (polygon[j][1] > point[1])) // One point needs to be above, one below our y coordinate
            // ...and the edge doesn't cross our Y corrdinate before our x coordinate (but between our x coordinate and infinity)
            && (point[0] < ((polygon[j][0] - polygon[i][0]) * (point[1] - polygon[i][1]) / (polygon[j][1] - polygon[i][1]) + polygon[i][0]))) {
            // Invert odd
            odd = !odd;
        }

        j = i;

    }
    //If the number of crossings was odd, the point is in the polygon
    return odd;
};

function coastlinesIntersect() {
    // SELECT geom FROM coastlines WHERE ST_INTERSECTS(ST_GeomFromText('POLYGON ((-76.80542 25.383735, -76.234131 25.383735, -76.234131 25.799891, -76.80542 25.799891, -76.80542 25.383735))'), geom) LIMIT 1;
}
