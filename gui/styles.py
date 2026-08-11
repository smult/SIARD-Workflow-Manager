"""gui/styles.py — felles farger og fonter for hele GUI.

Støtter to temaer: 'dark' (standard) og 'light'. COLORS-dicten oppdateres
in-place ved tema-bytte (alle moduler som har gjort `from gui.styles
import COLORS` vil se nye verdier).  Widgets som allerede er bygget med
en eksplisitt `fg_color=COLORS[...]`-overstyring beholder gamle farger
til de bygges på nytt — derfor anbefales restart for fullt tema-bytte.
CustomTkinter sine innebygde widgets oppdateres umiddelbart via
`ctk.set_appearance_mode()`.
"""

# ── Mørkt tema (standard) ─────────────────────────────────────────────────────
COLORS_DARK = {
    "bg":         "#0d0f14",
    "surface":    "#13161e",
    "panel":      "#191d28",
    "border":     "#252b3a",
    "dropzone":   "#1a2640",   # blåtonet drop-zone, skilt fra panel
    "accent":     "#4f8ef7",
    "accent_dim": "#3a70d4",
    "green":      "#2ecc71",
    "yellow":     "#f0c040",
    "red":        "#e05252",
    "muted":      "#8a95b0",
    "text":       "#d4daf0",
    "text_sub":   "#b7bcc8",
    "btn":        "#1e2333",
    "btn_hover":  "#252b3a",
    # Sekundær-knapp (tekst/kant) + tekst på primær (accent) knapp
    "btn_text":   "#d4daf0",
    "btn_border": "#252b3a",
    "on_accent":  "#ffffff",
    # Destruktiv knapp (slett/fjern/stopp)
    "danger_bg":     "#2a1515",
    "danger_hover":  "#3d2020",
    "danger_text":   "#e05252",
    "danger_border": "#3a1c1c",
    # Tabview / segmentknapp (operasjons-faner). Delt tekstfarge for valgt+uvalgt,
    # så valgt-bakgrunnen er dempet nok til at lys tekst består AA.
    "tab_bar_bg":            "#191d28",   # stripe/gap mellom faner
    "tab_unselected_bg":     "#232838",   # synlig, mørk fane-pille
    "tab_unselected_hover":  "#2c3346",
    "tab_selected_bg":       "#295291",   # dempet blå → #d4daf0 = 5.6:1 (AA)
    "tab_selected_hover":    "#2f5aa0",
    "tab_text":              "#d4daf0",
    "cat_Integritet":  "#4f8ef7",
    "cat_Innhold":     "#f0c040",
    "cat_Validering":  "#2ecc71",
    "cat_Metadata":    "#a78bfa",
    "cat_Sikkerhet":   "#e05252",
    "cat_Rapport":     "#f97316",
    "cat_Kontroll":    "#22d3ee",
    "cat_Analyse":     "#fb7185",
}

# ── Lyst tema ────────────────────────────────────────────────────────────────
# NB: Sekundærknapper bruker et "tonalt" uttrykk i lyst tema — lyst tonet fyll
# (`btn`) + mørk blå tekst (`btn_text`) + tynn kant (`btn_border`). Dette gir
# WCAG-AAA-kontrast og et lettere, mer moderne uttrykk enn en mørk slate-flate.
# Primærknapper (accent) bruker ren hvit tekst (`on_accent`) for å bestå AA.
COLORS_LIGHT = {
    "bg":         "#eef1f6",
    "surface":    "#f6f8fb",
    "panel":      "#ffffff",
    "border":     "#cdd3e0",
    "dropzone":   "#dde6f5",
    "accent":     "#2563eb",   # dypere blå for kontrast mot lys bg
    "accent_dim": "#1d4ed8",
    "green":      "#15803d",
    "yellow":     "#b45309",
    "red":        "#dc2626",
    "muted":      "#6b7280",
    "text":       "#1a1f2e",
    "text_sub":   "#3b4252",
    "btn":        "#e8eefc",   # tonalt lyst fyll (sekundærknapp)
    "btn_hover":  "#dbe6fb",
    "btn_text":   "#1e40af",   # mørk blå knappetekst (AAA mot btn)
    "btn_border": "#c3d4fb",   # tynn kant som avgrenser mot lys bakgrunn
    "on_accent":  "#ffffff",   # ren hvit tekst på primær accent-knapp
    # Destruktiv knapp — tonalt rødt fyll i stedet for mørk maroon-blokk
    "danger_bg":     "#fdeaea",
    "danger_hover":  "#fbdada",
    "danger_text":   "#991b1b",
    "danger_border": "#f4c7c7",
    # Tabview: lyse fane-piller + mørk tekst (AAA), tydeligere valgt-farge
    "tab_bar_bg":            "#ffffff",
    "tab_unselected_bg":     "#dde3ee",
    "tab_unselected_hover":  "#cfd8ea",
    "tab_selected_bg":       "#a9c8f5",
    "tab_selected_hover":    "#93b8f0",
    "tab_text":              "#1a1f2e",
    "cat_Integritet":  "#2563eb",
    "cat_Innhold":     "#b45309",
    "cat_Validering":  "#15803d",
    "cat_Metadata":    "#7c3aed",
    "cat_Sikkerhet":   "#dc2626",
    "cat_Rapport":     "#ea580c",
    "cat_Kontroll":    "#0891b2",
    "cat_Analyse":     "#e11d48",
}

# Aktiv palett — starter med dark, oppdateres via apply_theme()
COLORS = dict(COLORS_DARK)

FONTS = {
    "mono": "Courier New",   # fallback hvis Courier finnes
    "ui":   "Segoe UI",
}

# Logg-farger (per nivå) — har egne paletter per tema
_LOG_COLORS_DARK = {
    "info":    "#9aa4bc",
    "step":    "#4f8ef7",
    "success": "#2ecc71",
    "warn":    "#f0c040",
    "error":   "#e05252",
    "muted":   "#5c6880",
}

_LOG_COLORS_LIGHT = {
    "info":    "#4b5563",
    "step":    "#2563eb",
    "success": "#15803d",
    "warn":    "#b45309",
    "error":   "#dc2626",
    "muted":   "#9ca3af",
}

LOG_COLORS = dict(_LOG_COLORS_DARK)


def apply_theme(mode: str) -> str:
    """
    Bytt aktivt tema. `mode` = "dark" eller "light".
    Oppdaterer COLORS- og LOG_COLORS-dictene in-place og setter
    CustomTkinter sitt appearance_mode.

    Returnerer faktisk satt modus (sanitisert).
    """
    mode = (mode or "dark").lower().strip()
    if mode not in ("dark", "light"):
        mode = "dark"

    src        = COLORS_LIGHT if mode == "light" else COLORS_DARK
    log_src    = _LOG_COLORS_LIGHT if mode == "light" else _LOG_COLORS_DARK

    COLORS.clear()
    COLORS.update(src)
    LOG_COLORS.clear()
    LOG_COLORS.update(log_src)

    try:
        import customtkinter as _ctk
        _ctk.set_appearance_mode(mode)
    except Exception:
        pass

    return mode


def current_theme() -> str:
    """Returnerer 'dark' eller 'light' basert på nåværende COLORS."""
    return "light" if COLORS.get("bg") == COLORS_LIGHT["bg"] else "dark"

def cat_color(category: str) -> str:
    return COLORS.get(f"cat_{category}", COLORS["accent"])


# ── Font-skalering ────────────────────────────────────────────────────────────

FONT_MIN_SIZE = 10   # tidl. 9; bumpes til 10 som minimum for alle tekster


class FontRegistry:
    """Holder styr på alle CTkFont-instanser for dynamisk størrelsesjustering."""
    _fonts: list = []       # [(weakref(font), base_size)]
    _observers: list = []   # [weakref.WeakMethod(callback)] — kalles ved endring
    _offset: int = 0

    @classmethod
    def add_observer(cls, callback) -> None:
        """
        Registrer en callback (typisk en bundet metode) som kalles hver gang
        font-størrelsen endres. Brukes av widgets som IKKE bygger på CTkFont —
        f.eks. canvas-tegnet tekst — og som selv må tegne om ved endring.
        Holdes via WeakMethod slik at widgeten kan samles av GC.
        """
        import weakref
        try:
            ref = weakref.WeakMethod(callback)
        except TypeError:
            ref = weakref.ref(callback)   # frittstående funksjon
        cls._observers.append(ref)

    @classmethod
    def _notify(cls) -> None:
        dead = []
        for i, wr in enumerate(cls._observers):
            cb = wr()
            if cb is None:
                dead.append(i)
            else:
                try:
                    cb()
                except Exception:
                    pass
        for i in reversed(dead):
            del cls._observers[i]

    @classmethod
    def _apply(cls) -> None:
        dead = []
        for i, (wr, base) in enumerate(cls._fonts):
            f = wr()
            if f is None:
                dead.append(i)
            else:
                try:
                    f.configure(size=max(FONT_MIN_SIZE, base + cls._offset))
                except Exception:
                    pass
        for i in reversed(dead):
            del cls._fonts[i]
        cls._notify()

    @classmethod
    def effective_size(cls, base_size: int) -> int:
        """Effektiv fontstørrelse for en gitt base-størrelse ved gjeldende offset.
        Samme formel som CTkFont-wrapperen bruker, slik at canvas-tekst matcher."""
        return max(FONT_MIN_SIZE, base_size + cls._offset)

    @classmethod
    def scale(cls, delta: int) -> None:
        cls._offset = max(-3, min(8, cls._offset + delta))
        cls._apply()

    @classmethod
    def current_offset(cls) -> int:
        return cls._offset

    @classmethod
    def set_offset(cls, offset: int) -> None:
        cls._offset = max(-3, min(8, int(offset)))
        cls._apply()


def _install_font_wrapper() -> None:
    """Monkey-patch ctk.CTkFont så alle instanser auto-registreres."""
    import customtkinter as ctk
    import weakref
    if getattr(ctk, "_font_wrapper_installed", False):
        return
    _Orig = ctk.CTkFont

    def _make_font(family=None, size=12, weight="normal", **kw):
        actual = max(FONT_MIN_SIZE, size + FontRegistry._offset)
        f = _Orig(family=family, size=actual, weight=weight, **kw)
        FontRegistry._fonts.append((weakref.ref(f), size))
        return f

    ctk.CTkFont = _make_font
    ctk._font_wrapper_installed = True


_install_font_wrapper()
