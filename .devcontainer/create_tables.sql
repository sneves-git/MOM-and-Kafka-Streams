CREATE TABLE location (
    name VARCHAR(512),
    PRIMARY KEY(name)
);

CREATE TABLE weatherstation (
    name VARCHAR(512),
    PRIMARY KEY(name)
);

CREATE TABLE weatherstationresults (
    weatherstation         VARCHAR(512),
    1_tempcount         BIGINT,
    3_tempmin         BIGINT,
    3_tempmax         BIGINT,
    5_numofalerts         BIGINT,
    7_mintempwithredalerts     BIGINT,
    9_tempmininredzone     BIGINT,
    10_avgtemp         BIGINT,
    11_avgtempwithredlasthour BOOL,
    PRIMARY KEY(weatherstation)
);

CREATE TABLE locationresults (
    location             VARCHAR(512),
    2_tempcount         BIGINT,
    4_tempmax             BIGINT,
    4_tempmin             BIGINT,
    8_maxtempwithalertslasthour BIGINT,
    PRIMARY KEY(location)
);

CREATE TABLE alerts (
    redcount     BIGINT,
    greencount BIGINT
);