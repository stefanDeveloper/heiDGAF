@load base/protocols/dns
module CustomDNS;
export {
    redef enum Log::ID += { LOG };
    type Info: record {
        ts: string &log;
        uid: string &log;
        src_ip: addr &log;
        src_port: port &log;
        dns_server_ip: addr &log;
        dns_server_port: port &log;
        domain_name: string &log &optional;
        record_type: string &log &optional;
        response_ip: vector of string &log &optional;
        ttls: vector of interval &log &optional;
        rejected: bool &log &default=F;
        status_code_id: count &log &optional;
        status_code: string &log &optional;

    };
    global log_dns: event(rec: Info);
    global dns_payload_sizes: table[string] of count
        &default=0
        &write_expire = 5min;
}



event zeek_init() &priority=5
{
    Log::create_stream(CustomDNS::LOG, [$columns=Info, $path="custom_dns"]);
}


redef record CustomDNS::Info += {
    size: count &log &optional;
};

event dns_message(c: connection, is_query: bool, msg: dns_msg, len: count)
{
    dns_payload_sizes[c$uid] = len;
}

event DNS::log_dns(rec: DNS::Info)
{
    local dnsLog: Info = [
        $ts = strftime("%Y-%m-%dT%H:%M:%S", rec$ts),
        $uid = rec$uid,
        $src_ip = rec$id$orig_h,
        $src_port = rec$id$orig_p,
        $dns_server_ip = rec$id$resp_h,
        $dns_server_port = rec$id$resp_p,
        $rejected = rec$rejected
    ];

    ##### add custom log messages if a given field that needs to be present is not present in the logline ####
    # use this only for fields that are absolutely necessary

    # Keep this deactivated for now, as we want to use zeek at first to not prefilter anything

#    if ( ! rec?$query )
#        print fmt("Info: missing domain in DNS log %s, skipping the log...", rec);
#    if ( ! rec?$conn )
#        print fmt("Info:could not determine request length for line  %s, skipping the log...", rec);
#
    ###########################################################################################################

    if ( rec?$query )
        dnsLog$domain_name = rec$query;

    if ( rec?$qtype_name )
        dnsLog$record_type = rec$qtype_name;

    if ( rec?$answers )
        dnsLog$response_ip = rec$answers;

    if ( rec?$TTLs )
        dnsLog$ttls = rec$TTLs;

    if ( rec?$rcode )
        dnsLog$status_code_id = rec$rcode;

    if ( rec?$rcode_name )
        dnsLog$status_code = rec$rcode_name;

    if ( rec$uid in dns_payload_sizes )
        {
        dnsLog$size = dns_payload_sizes[rec$uid];
        delete dns_payload_sizes[rec$uid];
        }

    Log::write(CustomDNS::LOG, dnsLog);
}
