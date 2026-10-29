import "@antadesign/anta/bundle.css";
import "@antadesign/anta/theme-antithesis.css";
import "./styles.css";
import "@antadesign/anta/bundle";
import { createRoot } from "react-dom/client";
import { App } from "./App.tsx";

createRoot(document.getElementById("root")!).render(<App />);
