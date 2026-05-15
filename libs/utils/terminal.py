from rich.theme import Theme
from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from rich.align import Align
from rich.rule import Rule

# ATLAS Cyber-Minimalist Theme
ATLAS_THEME = Theme({
    "info": "cyan",
    "warning": "yellow",
    "error": "bold red",
    "success": "bold green",
    "highlight": "bold magenta",
    "muted": "dim white",
    "brand": "bold bright_blue",
    "accent": "bold #39FF14" # High-fidelity Neon Green
})

console = Console(theme=ATLAS_THEME)

def print_header(subtitle: str = "SYSTEMS ONLINE"):
    banner = """
    [brand]
    ___  _____ _      ___   _____ 
   / _ \|_   _| |    / _ \ /  ___|
  / /_\ \ | | | |   / /_\ \\ `--. 
  |  _  | | | | |   |  _  | `--. \\
  | | | | | | | |___| | | |/\__/ /
  \_| |_/ \_/ \_____/\_| |_/\____/ [/]
    [muted]DISTRIBUTED WEB INTELLIGENCE ENGINE[/]
    """
    console.print(Align.center(banner))
    console.print(Rule(subtitle, style="brand"))
    console.print("\n")

def print_step(message: str, status: str = "info"):
    icon = "->"
    if status == "success": icon = "OK"
    elif status == "error": icon = "ERROR"
    elif status == "warning": icon = "!"
    
    console.print(f"[{status}]{icon} {message}[/]")
