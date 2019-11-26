const {Client} = require('pg');
const express = require('express');

Date.prototype.withoutTimezone = function () {
    this.setMilliseconds(this.getMilliseconds() - this.getTimezoneOffset() * 1000 * 60);
    return this;
};

Array.prototype.mapDateTimezone = function (key) {
    return this.map((data) => {
        data[key] = new Date(data[key]).withoutTimezone();
        return data
    });
};

Array.prototype.groupByKey = function (key) {
    return this.reduce(function (r, a) {
        r[a[key]] = r[a[key]] || [];
        r[a[key]].push(a);
        return r;
    }, Object.create(null));
};

const app = express();
app.use(express.json());
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    next();
});

const PORT = process.env.PORT || 8080;

const client = new Client({
    user: "postgres",
    password: "deszczomierz",
    host: "localhost",
    port: 5432,
    database: "deszczomierz_db"
});

client.setTypeParser(1082, function (stringValue) {
    return stringValue;
});

app.listen(PORT, () => console.log(`My server is listening on port ${PORT}`));

app.get('/all', async (req, res) => {
    const rows = await readAll();
    res.send(rows)
});

app.get('/lastHour', async (req, res) => {
    const rows = await readLastHour();
    res.send(rows)
});

app.get('/lastDay', async (req, res) => {
    const rows = await loadLastDay();
    res.send(rows)
});

app.get('/lastDayArrays', async (req, res) => {
    const rows = await loadLastDayArrays();
    res.send(rows)
});

app.get('/lastWeek', async (req, res) => {
    const rows = await loadLastWeek();
    res.send(rows)
});

app.get('/lastWeekArrays', async (req, res) => {
    const rows = await loadLastWeekArrays();
    res.send(rows)
});

app.post('/all', async (req, res) => {
    let result = {};
    try {
        console.log(req.body);
        const reqJson = req.body;
        await addMeasurement(reqJson.id, reqJson.measurement, reqJson.date, reqJson.time, reqJson.date_time);
        result.success = true
    } catch (e) {
        result.success = false
    } finally {
        res.send(result)
    }
});

connect();

async function connect() {
    try {
        await client.connect();
    } catch (e) {
        console.error("Failed to connect to Postgres DB due to:" + e)
    }
}

async function addMeasurement(id, measurement, date, time, date_time) {
    try {
        await client.query("insert into rain_gauge_data (id, measurement, date, time, date_time) values ($1, $2, $3, $4, $5)", [id, measurement, date, time, date_time]);
        return true
    } catch (e) {
        return false
    }
}

async function readAll() {
    try {
        const results = await client.query("select * from rain_gauge_data");
        return results.rows
    } catch (e) {
        return []
    }
}

async function readLastHour() {
    try {
        const results = await client.query("select coalesce(sum(measurement), 0) as suma from rain_gauge_data where date_time >= now() - interval '1 hour'");
        return results.rows
    } catch (e) {
        return []
    }
}

async function loadLastDay() {
    try {
        const results = await client.query("select series as date_time, cast(series::timestamp as time) as hour, coalesce(sum(measurement), 0) as suma from generate_series( date_trunc('hour', now()::timestamp) - '23 hours'::interval, date_trunc('hour', now()::timestamp), '1 hour'::interval ) as series left join rain_gauge_data on date_trunc('hour', rain_gauge_data.date_time) = series and now()::timestamp >= date_time - INTERVAL '1 DAY' group by 1 order by series");
        return results.rows.mapDateTimezone("date_time");
    } catch (e) {
        return []
    }
}

async function loadLastDayArrays() {
    try {
        const results = await client.query("select P.temp, array_agg(date_part('hour', P.hour)) as hours, array_agg(P.suma) as values from (select series, date_part('minute', series) as temp, cast(series::timestamp as time) as hour, coalesce(sum(measurement), 0) as suma from generate_series( date_trunc('hour', now()::timestamp) - '23 hours'::interval, date_trunc('hour', now()::timestamp), '1 hour'::interval ) as series left join rain_gauge_data on date_trunc('hour', rain_gauge_data.date_time) = series and now()::timestamp >= date_time - INTERVAL '1 DAY' group by 1 order by series) as P group by P.temp");
        return results.rows
    } catch (e) {
        return []
    }
}

async function loadLastWeek() {
    try {
        const results = await client.query("select T.date, T.hour, T.date + T.hour as date_time, coalesce(sum(measurement), 0) as suma from (select DATE(date) as date, cast (hour::timestamp as time) as hour, DATE(date) + cast (hour::timestamp as time) as date_time from generate_series( current_date - '7 days'::interval, current_date, '1 day'::interval ) as date cross join generate_series(date_trunc('day', now()) - '23 hours'::interval, date_trunc('day', now()),    '1 hour'::interval   ) as hour) AS T left join rain_gauge_data on date_trunc('hour', rain_gauge_data.time) = T.hour and T.date = rain_gauge_data.date group by T.date, T.hour order by T.date, T.hour");
        return results.rows.mapDateTimezone("date_time").groupByKey("date");
    } catch (e) {
        return []
    }
}

async function loadLastWeekArrays() {
    try {
        const results = await client.query("select P.date, array_agg(date_part('hour', P.hour)) as hours, array_agg(P.suma) as values from (select T.date, T.hour, coalesce(sum(measurement), 0) as suma from (select DATE(date) as date, cast (hour::timestamp as time) as hour, DATE(date) + cast (hour::timestamp as time) as date_time from generate_series( current_date - '6 days'::interval, current_date, '1 day'::interval ) as date cross join generate_series(date_trunc('day', now()) - '23 hours'::interval, date_trunc('day', now()),'1 hour'::interval   ) as hour) AS T left join rain_gauge_data on date_trunc('hour', rain_gauge_data.time) = T.hour and T.date = rain_gauge_data.date group by T.date, T.hour order by T.date, T.hour) as P group by P.date order by date desc");
        return results.rows
    } catch (e) {
        return []
    }
}

