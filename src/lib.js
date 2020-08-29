'use strict';

function encodeUriParams(params) {
    return Object.keys(params)
        .map(k => encodeURIComponent(k) + '=' + encodeURIComponent(params[k]))
        .join('&');
}

const flood = {
    doGet(args) {
        let path = ('path' in args) ? args.path : '/';
        let headers = ('headers' in args) ? args.headers : {};
        if ('qs' in args) {
            path += '?' + encodeUriParams(args.qs);
        }
        return { method: 'GET', path: path, headers: headers, body: '' };
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
