var productLocal = 'LOCAL';
var productLinux = 'LINUX';
var productWindows = 'WINDOWS';

module.exports = {

    PRODUCTION: productLocal,
    // PRODUCTION: productLinux,
    // PRODUCTION: productWindows,

    PRODUCTION_LOCAL: productLocal,
    PRODUCTION_LINUX: productLinux,
    PRODUCTION_WINDOWS: productWindows,

    DATABASE_URL_LOCAL: 'postgres://postgres:123456@localhost/bustabitdb', // database url for local development
    DATABASE_URL_LINUX: 'postgres://postgres:123456@47.75.43.93/bustabitdb', // database url for linux server - test
    DATABASE_URL_WINDOWS: 'postgres://postgres:bmUgswMNVK9n4J7S@172.17.0.6/bustabitdb', // database url for windows server - production

    PORT_HTTP_W: 80, // http web server port
    PORT_HTTPS_W: 443, // https web server port
    PORT_HTTP_G: 3880, // http game server port
    PORT_HTTPS_G: 3443, // https game server port
    HTTPS_KEY: './ssl/private.key', // ssl key for https server
    HTTPS_CERT: './ssl/certificate.crt',
    HTTPS_CA: './ssl/ca_bundle.crt',
    ENC_KEY: 'enc_key_Bio',

    GAME_CLOSE: false,
    // Do not set any of this on production

    CRASH_AT: undefined// Force the crash point
};
