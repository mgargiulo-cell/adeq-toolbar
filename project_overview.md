---
name: ADEQ Toolbar — Project Overview
description: Chrome extension for automated B2B sales prospecting at ADEQ Media (programmatic advertising company)
type: project
---

ADEQ Toolbar is a Chrome Extension (Manifest v3) for automated lead prospecting and qualification at ADEQ Media, a programmatic advertising company. The extension analyzes websites to identify sales opportunities.

**Version:** 3.0.0  
**Language:** Vanilla JavaScript ES6+ (no build step, no bundler)  
**Entry point:** popup/popup.html → popup/popup.js

**Core team using the tool:** Agus, Diego, Max (media buyers)  
**Default media buyer:** "Agus" in state, set from login email  
**Traffic threshold:** 500,000 pageviews/month minimum  

**Why:** ADEQ Media needs to qualify publisher prospects quickly. The extension centralizes data collection (traffic, tech stack, email) and CRM push (Monday.com) in one click.

**How to apply:** When adding features, keep the no-build-step pattern — pure ES6 modules, no transpilation, no npm packages.
