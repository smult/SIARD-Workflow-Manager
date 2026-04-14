"""gui/styles.py — felles farger og fonter for hele GUI."""

COLORS = {
    "bg":         "#0d0f14",
    "surface":    "#13161e",
    "panel":      "#191d28",
    "border":     "#252b3a",
    "accent":     "#4f8ef7",
    "accent_dim": "#3a70d4",
    "green":      "#2ecc71",
    "yellow":     "#f0c040",
    "red":        "#e05252",
    "muted":      "#5a637a",
    "text":       "#d4daf0",
    "text_sub":   "#7a849e",
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
    "info":    "#7a849e",
    "step":    "#4f8ef7",
    "success": "#2ecc71",
    "warn":    "#f0c040",
    "error":   "#e05252",
    "muted":   "#3d4560",
}

def cat_color(category: str) -> str:
    return COLORS.get(f"cat_{category}", COLORS["accent"])
