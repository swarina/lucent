// Scaffold shell. The real screens (Theater, Inspector, Ops, Ingest) land with
// M0-T10 and M1-T8 per docs/frontend.md. This placeholder proves the toolchain
// and pins the visual language (tokens in index.css) from the first commit.

export function App() {
  return (
    <main className="shell">
      <h1>
        Lucent<span className="cursor" />
      </h1>
      <p className="tagline">a distributed vector search engine you can watch think</p>
      <p className="status">scaffold — the live interface arrives at milestone M0-T10.</p>
    </main>
  );
}
