# Dockerfile
FROM crpi-lmega5fbvej4u3db.cn-shanghai.personal.cr.aliyuncs.com/novigrad/eda:v0.4


USER 0

COPY oss-cad-suite-linux-x64-20260920.tgz /tmp/oss-cad-suite.tgz

RUN mkdir -p /opt \
    && tar -xzf /tmp/oss-cad-suite.tgz -C /opt \
    && rm -f /tmp/oss-cad-suite.tgz \
    && test -x /opt/oss-cad-suite/bin/yosys \
    && /opt/oss-cad-suite/bin/yosys -V

ENV PATH="/opt/oss-cad-suite/bin:${PATH}"

RUN command -v yosys \
    && yosys -V