// Whether an event carries text that holds every needle.
//
// snouty applies this to `(ev, needles)`, with the needles lowercased: `m`
// folds only the haystack.
(ev, needles) => {
  const m = (s, needle) => s != null && String(s).toLowerCase().includes(needle);
  const assert_hit = ev.antithesis_assert?.hit === true;
  const not_catalog = !ev.antithesis_assert || ev.antithesis_assert.hit !== false;
  const composer = ev.source?.name === "antithesis_test_composer"
    || /^antithesis\/pods\/.*\/commands/.test(ev.source?.name ?? "");
  const has = (needle) =>
    (not_catalog && m(ev.output_text, needle))
    || (assert_hit
      && (m(ev.antithesis_assert.message, needle)
        || m(ev.antithesis_assert.location?.function, needle)))
    || (composer
      && (m(ev.source?.name, needle)
        || m(ev.command, needle)
        || m(ev.started_task, needle)));
  return needles.every(has);
}
