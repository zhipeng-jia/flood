'use strict';

function encodeUriParams(params) {
    return Object.keys(params)
        .map(k => encodeURIComponent(k) + '=' + encodeURIComponent(params[k]))
        .join('&');
}

const allDigits = [..."0123456789"];
const allCapsAlpha = [..."ABCDEFGHIJKLMNOPQRSTUVWXYZ"];
const allLowerAlpha = [..."abcdefghijklmnopqrstuvwxyz"];
const allAlpha = [...allCapsAlpha, ...allLowerAlpha];
const allAlphaDigits = [...allAlpha, ...allDigits];

function randomString(base, length) {
    let result = "";
    for (let i = 0; i < length; i++) {
        let idx = Math.random() * base.length | 0;
        result += base[idx];
    }
    return result;
}

const flood = {
    // Random integer within [a, b)
    randInt(a, b) {
        return (Math.random() * (b-a) | 0) + a;
    },

    randDigitString(length) {
        return randomString(allDigits, length);
    },

    randLowercaseString(length) {
        return randomString(allLowerAlpha, length);
    },

    randUppercaseString(length) {
        return randomString(allCapsAlpha, length);
    },

    randAlphabetString(length) {
        return randomString(allAlpha, length);
    },

    randAlphabetDigitString(length) {
        return randomString(allAlphaDigits, length);
    },

    doGet(args) {
        let path = ('path' in args) ? args.path : '/';
        let headers = ('headers' in args) ? args.headers : {};
        if ('qs' in args) {
            path += '?' + encodeUriParams(args.qs);
        }
        return { method: 'GET', path: path, headers: headers };
    },

    doPost(args) {
        let path = ('path' in args) ? args.path : '/';
        let headers = ('headers' in args) ? args.headers : {};
        let body = '';
        if ('qs' in args) {
            path += '?' + encodeUriParams(args.qs);
        }
        if ('params' in args) {
            headers['Content-Type'] = 'application/x-www-form-urlencoded';
            body = encodeUriParams(args.params);
        } else if ('json' in args) {
            headers['Content-Type'] = 'application/json';
            body = JSON.stringify(args.json);
        }
        return { method: 'POST', path: path, headers: headers, body: body };
    }
};
