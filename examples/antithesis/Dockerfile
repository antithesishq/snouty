# Two images from one file: `bank` runs the server, and `workload` adds the
# Test Composer commands that drive it. Only the workload image holds the
# commands, so Antithesis runs them in the workload container alone.
FROM python:3.12-slim AS bank

RUN pip install --no-cache-dir antithesis==0.4.0
COPY bank/ /opt/bank/
# Antithesis catalogs the SDK assertions in the Python files under
# /opt/antithesis/catalog, so properties that never run still appear in the
# report.
RUN mkdir -p /opt/antithesis/catalog /var/lib/bank \
    && ln -s /opt/bank /opt/antithesis/catalog/bank
ENV PYTHONPATH=/opt/bank PYTHONUNBUFFERED=1

FROM bank AS workload

# Test Composer commands: /opt/antithesis/test/v1/<test name>/<command>.
COPY test/ /opt/antithesis/test/
RUN ln -s /opt/antithesis/test/v1/bank /opt/antithesis/catalog/commands
