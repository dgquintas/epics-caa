addpath('./jsonlab')
addpath('./urlread2')

% this should be configurable from the outside somehow
serverurl = 'http://localhost:8888' ;

% subscribe to something
pvname = 'test:long1';
mode = struct('mode', 'Monitor', 'delta', 10);
subs_info = struct('pvname', pvname, 'mode', mode);
subs_info_json = strcat('[', savejson('', subs_info), ']') % ugly ugly hack
url = strcat(serverurl,'/subscriptions/')
[output, extras] = urlread2(url, 'POST', subs_info_json)

% unsubscribe
pvname = 'test:long1';
url = strcat(serverurl,'/subscriptions/', pvname)
[output, extras] = urlread2(url, 'DELETE')


%% get values
%pvname = 'test:long1';
%params = {'limit', '5'}
%[queryString, header] = http_paramsToString(params,1)
%url = strcat(serverurl,strcat('/values/', pvname))
%[output, extras] = urlread2([url '?' queryString], 'GET')
%output = loadjson(output)
