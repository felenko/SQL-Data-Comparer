Overall visual style
Dark graphite / blue-gray theme
Slightly textured background, like brushed or cloudy metal
Low-saturation colors
Thin borders and subtle separators
Slight gradients on panels and buttons
Compact spacing, dense but readable
Strong desktop-tool feeling, not rounded playful SaaS style
Mostly square corners or very slightly rounded corners, around 2–4 px max
Use soft inner shadows and faint outer highlights to create depth
Typography should be clean and utilitarian, similar to Segoe UI
Font sizes should be modest: around 12–14 for normal text, 16–18 for section headers
Main layout

The window is divided into these areas:

1. Top title bar / app chrome
Custom dark title bar
Left side: app name “SQL Data Compare”
Right side: standard minimize, maximize, close buttons
Very subtle gradient from darker top edge to slightly lighter lower edge
Thin bottom border line
2. Top tab strip

Below the title bar, place a horizontal tab strip.
Tabs look like classic desktop tool tabs, not browser tabs.

Example tabs:

Tables
Views
Stored Procedures
Functions

For the data-compare version, the first tab “Tables” is active.

Tab style:

Inactive tabs: darker background, muted text
Active tab: slightly lighter panel tone, stronger text, looks pressed/selected
Thin border around each tab
Compact height
3. Filter/action bar

Below tabs, a horizontal toolbar row:

Left side:
label like “Show:”
compact dropdown for filter mode: Different / Missing / Identical / Filtered
Right side:
summary text like “12 Differences Found: 3 Tables, 12 Rows”
primary action button: “Synchronize”

Toolbar style:

Slightly lighter than outer background
Flat-dark desktop toolbar look
Subtle top highlight and bottom border
Main content area

Split into two columns:

Left panel: table list

A narrow navigation panel listing tables with differences.

Panel style:

Dark panel background with slightly lighter header
Header text: “Table Differences”
Optional search/filter icon on the header right
Thin vertical separator between left and right panels

Each table row in the list:

Full-width row
Compact height
Has icon/status color at left
Table name bold enough to scan quickly
Difference summary aligned right, e.g.:
“3 Row Differences”
“Missing Table”
“8 Row Differences”

Status colors:

Red = changed / conflict / missing row on one side
Green = added row
Amber/yellow = warning / mixed differences / partial mismatch

Selected row:

Slightly brighter background
Thin border or glow edge
Keep it desktop-like, not neon

Example entries:

Customers
Orders
Products
Right panel: selected table diff

This is the main comparison workspace.

At the top:

Centered or left-aligned title:
“Comparing Table: Customers”
Slightly larger text
Clean spacing above the grid

Then show one main group box / section:

Section header: “Row Differences”

The right side should focus on data only, not schema.

Row difference grid

Main grid is the most important part.

Grid structure

Columns:

change indicator
ID
Name
Email
Status
optional notes/result column

Grid look:

Desktop DataGrid appearance
Dark header row with slightly lighter fill than body
Thin grid lines
Row height compact but readable
Use alternating very subtle row shading
Text aligned cleanly
Important values easy to scan
Difference visualization

Use row-level and cell-level highlighting.

Row types
Changed row
Red-tinted row background, but muted
Only changed cells should be more strongly highlighted
Change indicator icon on far left, such as ≠ or edit marker
Added row
Green-tinted row background
Indicator icon: plus
Means exists in target but not source, or vice versa depending on context
Missing/deleted row
Red or dark crimson tint
Indicator icon: minus or missing marker
Unchanged cells inside changed rows
Keep dark neutral background so changed cells stand out
Example data
Row 102: Alice Smith, alice@example.com
, Status differs
Row 105: New Entry, newuser@test.com
, Added
Row 204: Mike Johnson, john@test.com
, Missing
Cell emphasis
Changed cells should have stronger fill than unchanged cells
Example:
green fill for value added
red fill for value removed/missing
amber for modified value comparison
Keep fills muted and professional, not bright saturated colors
Bottom navigation bar

At the bottom of the window:

Left button: “Previous Difference”
Center: record position like “5 of 12” with left/right small arrow controls
Right button: “Next Difference”

Style:

Dark command bar
Buttons are rectangular desktop-style
Slight gradient and border
Center pager area compact and aligned
Colors

Use approximately these tones:

Window background: very dark blue-gray, almost charcoal
Main panel background: #252B36 to #2B3140 range
Secondary panel/header: slightly lighter than panel body
Borders: thin, cool gray-blue
Text primary: soft off-white, not pure white
Text secondary: muted gray
Changed/danger: muted red
Added/success: muted green
Warning: muted amber
Accent button: subdued steel-blue

Avoid:

bright cyan
flat black
oversized rounding
web-style cards
colorful modern dashboards
Background texture

The overall background should have a subtle cloudy / brushed / smoky texture. It should not look like a photo. It should look like a layered combination of:

dark linear gradients
low-opacity noise
soft radial shadowing near edges
faint highlight in the center

The texture should be barely visible, only enough to prevent the UI from looking flat.

WPF implementation guidance
Use Border, Grid, DockPanel, and DataGrid
Use custom ControlTemplate for tabs and buttons
Use LinearGradientBrush for panels/buttons
Use very subtle DropShadowEffect sparingly
Prefer reusable brushes and styles in a ResourceDictionary
Use custom row/cell styles for diff states
Make the DataGrid look like a classic desktop tool, not default bright WPF

And here is a small WPF background style example you can give the coding agent too:

<SolidColorBrush x:Key="WindowBaseBrush" Color="#1F2430"/>

<LinearGradientBrush x:Key="WindowBackgroundBrush" StartPoint="0,0" EndPoint="0,1">
    <GradientStop Color="#2A3040" Offset="0"/>
    <GradientStop Color="#222734" Offset="0.45"/>
    <GradientStop Color="#1C212C" Offset="1"/>
</LinearGradientBrush>

<RadialGradientBrush x:Key="WindowTextureOverlayBrush" Center="0.5,0.35" RadiusX="0.9" RadiusY="0.8">
    <GradientStop Color="#22FFFFFF" Offset="0"/>
    <GradientStop Color="#0AFFFFFF" Offset="0.45"/>
    <GradientStop Color="#00000000" Offset="1"/>
</RadialGradientBrush>

<Style x:Key="TexturedWindowStyle" TargetType="Grid">
    <Setter Property="Background" Value="{StaticResource WindowBackgroundBrush}"/>
</Style>