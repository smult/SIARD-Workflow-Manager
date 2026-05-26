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
    # Tabview-bakgrunner (operasjons-faner og lignende)
    "tab_unselected_bg":  "#0d0f14",
    "tab_selected_bg":    "#4f8ef7",
    "tab_selected_hover": "#3a70d4",
    "tab_text":           "#d4daf0",
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
# NB: `btn` er bevisst mørk (slate) i lyst tema slik at default hvit knappetekst
# som CTk plukker fra "dark-blue" core-themet forblir leselig på alle 45+
# knappebruk uten å eksplisitt sette text_color overalt.
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
    "btn":        "#475569",   # mørk slate → hvit default-tekst blir leselig
    "btn_hover":  "#334155",
    # Tabview: lys valgt-bakgrunn + mørk tekst for å unngå white-on-white
    "tab_unselected_bg":  "#dde3ee",
    "tab_selected_bg":    "#bfd4f5",
    "tab_selected_hover": "#a8c3f0",
    "tab_text":           "#1a1f2e",
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
    _fonts: list = []   # [(weakref(font), base_size)]
    _offset: int = 0

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
