FROM cargo.caicloudprivatetest.com/caicloud/lvs:alpine3.7

COPY bin/linux_amd64/ipvsdr /root/ipvsdr
COPY build/ipvsdr/keepalived.tmpl /root/keepalived.tmpl
COPY build/ipvsdr/keepalived.conf /etc/keepalived/keepalived.conf

ENTRYPOINT ["/root/ipvsdr", "--debug"]
