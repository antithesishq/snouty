# Bake the Antithesis test commands into the image at the conventional path, so
# they travel with the image — no host bind mount. The test command keeps the
# non-executable mode it has in the build context.
FROM debian:bookworm-slim
COPY test/ /opt/antithesis/test/v1/
