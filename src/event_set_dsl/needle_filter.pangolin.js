(ev, needles) => {
  const m = (s, needle) => s != null && String(s).toLowerCase().includes(needle);
  const composer = ev.source?.name === "antithesis_test_composer"
    || ev.source?.is_test_composer_command === true;
  const has = (needle) =>
    m(ev.output_text, needle)
    || m(ev.antithesis_assert?.message, needle)
    || m(ev.antithesis_assert?.location?.function, needle)
    || (composer
      && (m(ev.source?.name, needle)
        || m(ev.command, needle)
        || m(ev.started_task, needle)));
  return needles.every(has);
}
