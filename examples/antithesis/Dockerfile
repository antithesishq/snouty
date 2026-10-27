# Two images from one file: `bank` runs the server, and `workload` holds the
# Test Composer commands that drive it. Only the workload image holds the
# commands, so Antithesis runs them in the workload container alone.
FROM python:3.12-slim AS base

RUN pip install --no-cache-dir antithesis==0.4.0
# Antithesis catalogs the SDK assertions in the Python files under
# /opt/antithesis/catalog, so properties that never run still appear in the
# report. Each image links only its own sources there.
RUN mkdir -p /opt/antithesis/catalog \
    && ln -s /opt/bank /opt/antithesis/catalog/bank
ENV PYTHONPATH=/opt/bank PYTHONUNBUFFERED=1

FROM base AS bank

RUN mkdir -p /var/lib/bank
COPY bank/server.py /opt/bank/

FROM base AS workload

COPY bank/bank_client.py bank/ready.py /opt/bank/
# Test Composer commands: /opt/antithesis/test/v1/<test name>/<command>.
COPY test/ /opt/antithesis/test/
RUN ln -s /opt/antithesis/test/v1/bank /opt/antithesis/catalog/commands
