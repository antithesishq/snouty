# Bake the Antithesis test commands into the init image at the conventional
# path, so they travel with the image — no host bind mount.
FROM debian:bookworm-slim
COPY test/ /opt/antithesis/test/v1/
