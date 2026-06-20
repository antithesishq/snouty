# Bake the Antithesis test commands into the image at the conventional path, so
# they travel with the image — no host bind mount. The same image works for
# `snouty validate` locally and for a real `snouty launch`.
FROM debian:bookworm-slim
COPY test/ /opt/antithesis/test/v1/
