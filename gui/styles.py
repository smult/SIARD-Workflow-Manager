"""gui/styles.py — felles farger og fonter for hele GUI."""

COLORS = {
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
    # Kategorifarge per operasjonsdtype
    "cat_Integritet":  "#4f8ef7",
    "cat_Innhold":     "#f0c040",
    "cat_Validering":  "#2ecc71",
    "cat_Metadata":    "#a78bfa",
    "cat_Sikkerhet":   "#e05252",
    "cat_Rapport":     "#f97316",
    "cat_Kontroll":    "#22d3ee",
    "cat_Analyse":     "#fb7185",
}

FONTS = {
    "mono": "Courier New",   # fallback hvis Courier finnes
    "ui":   "Segoe UI",
}

LOG_COLORS = {
    "info":    "#9aa4bc",
    "step":    "#4f8ef7",
    "success": "#2ecc71",
    "warn":    "#f0c040",
    "error":   "#e05252",
    "muted":   "#5c6880",
}

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
