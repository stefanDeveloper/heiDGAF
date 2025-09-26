@load base/protocols/http
module CustomHTTP;
export {
    redef enum Log::ID += { LOG };
    type Info: record {
        ts: string &log;
        uid: string &log;
        src_ip: addr &log;
        src_port: port &log;
        dst_ip: addr &log;
        dst_port: port &log;
        method: string &log;
        host: string &log &optional;
        uri: string &log;
        referrer: string &log &optional;
        version: string &log &optional;
        user_agent: string &log &optional;
        request_body_len: count &log;
        response_body_len: count &log;
        status_code: count &log &optional;
        status_msg: string &log &optional;
    };
    global log_http: event(rec: Info);
}
event zeek_init() &priority=5
{
    Log::create_stream(CustomHTTP::LOG, [$columns=Info, $path="custom_http"]);
}



event HTTP::log_http(rec: HTTP::Info)
{
    local mylog: Info = [
        $ts = strftime("%Y-%m-%dT%H:%M:%S", rec$ts),
        $uid = rec$uid,
        $src_ip = rec$id$orig_h,
        $src_port = rec$id$orig_p,
        $dst_ip = rec$id$resp_h,
        $dst_port = rec$id$resp_p,
        $method = rec$method,
        $uri = rec$uri,
        $request_body_len = rec$request_body_len,
        $response_body_len = rec$response_body_len
    ];


    ##### add custom log messages if a given field that needs to be present is not present in the logline ####

    if ( ! rec?$host )
        print fmt("Info: missing host name in HTTP log %s, skipping the log...", rec);
    if ( ! rec?$status_code )
        print fmt("Info: missing status code in HTTP log %s, skipping the log...", rec);
    ###########################################################################################################

    if ( rec?$host )
        mylog$host = rec$host;
    if ( rec?$version )
        mylog$version = rec$version;
    if ( rec?$referrer )
        mylog$referrer = rec$referrer;
    if ( rec?$user_agent )
        mylog$user_agent = rec$user_agent;
    if ( rec?$status_code )
        mylog$status_code = rec$status_code;
    if ( rec?$status_msg )
        mylog$status_msg = rec$status_msg;

    Log::write(CustomHTTP::LOG, mylog);
}
