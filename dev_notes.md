

## Product Pages

Retrieval of product page HTML may be accomplished by headless GET request or may require a web driver, depending on the label:

- Headless Request
  - ADSR Sounds
- Web Driver
  - Splice

### Product Page Test Data

#### ADSR

Expected Fields:

- Title
- Label
- Genre
  - Single genre visible, additional genres require hover over
- Cover image
- Description
- Contents

adsr-image_sounds-flute_2.html
`https://www.adsrsounds.com/product/presets/image-sounds-flute-2-samples-loops/`

adsr-seven_sounds-beautiful_nightmares.html
`https://www.adsrsounds.com/product/presets/seven-sounds-beautiful-nightmares-dark-pop-vocals-samples-loops/`

#### Splice


`https://splice.com/sounds/packs/splice/virtual-riot-sample-pack/samples`

## File Path Parsing

### Samples

#### Examples

---


`/Onlyxne - Loop Kit 2/LOOPS/Onlyxne ~ mercy leather 130 Abm.wav`
`/808 Mafia - Gezin x Prevade - Gung Ho/Melo Fly To The Moon 156bpm (prod. Gezin x Prevade).wav`

`/AG - Wavy Sample Pack 18/Melo Fly To The Moon 156bpm (prod. Gezin x Prevade).wav`
`/AG - Wavy Sample Pack 18/3. Sand - A.G. SAMPLE (BPM 140) - WAVY VOL. X.wav`

`/Apollo Sound - Elysian_Lo_Fi_Ambient_Samples/APOLLO_SOUND_Elysian_Lo_Fi_Ambient_Samples_MAIN/ELAS_Musical_Loops/ELAS_Melody_Loops/ELAS_90_Em_Neptune_Synth_Rhodes_Harp_Melody_2.wav`

`/Atomic Sounds - Ultimate Phonk House Sample Pack/Melody/ASUPHSP_120Bpm_Melody_Emin_07.wav`
`/Atomic Sounds - Riddim Toolkit Sample Pack/Bass Loops/150Bpm_AS_Riddim_Toolkit_Bass_Loop_F_05.wav`

`/Beat Butcha - Dangerous Fireworks 3/LOOPS/MELODIC/HAS_PIANO_11.wav`
`/Beat Butcha - Messages from Outerspace/OUTTER_SPACE_ECHOS (113).wav`

`/BFractal Music - Boom Bap Attack/INSTRUMENT LOOP/08 Saxophone 90bpm D# Deep Melodic.wav`
`/BFractal Music - Chill Out Sphere 2/INSTRUMENT LOOP/09 Synth Lead 90bpm Gm Analog Lead.wav`

`/Big Citi Loops - 70s Funk Awards 4/05_70s_Funk_Awards_4_F#maj_100bpm/05 - Guitar 2.wav`
`/Big Citi Loops - RnB Minor Keys 2/05 - Guitar 2.wav`

`/Capsun ProAudio - Bedroom Beats & Lofi Hip-Hop Vol 2/Melodic_Loops/Lofi_Piano_&_Keys/BB2_120_lofi_keys_rhodes_all_my_friends_G.wav`

`/Splice Sounds - Wuki presents Wukipack 1/WUKI_tonal/WUKI_synth/WUKI_synth_loops/WUKI_135_synth_loop_melodic_glasspad_Gmaj.wav`
`/Splice Sounds - Wuki presents Wukipack 1/WUKI_tonal/WUKI_synth/WUKI_synth_loops/WUKI_124_synth_loop_beepboops_C3.wav`
`/Splice Sounds - The Voice of DYSON 2/DYSON_vocal_loops/DYSON_vocal_loops_wet/DYSON_128_vocal_loop_wet_dark_chop_A#min.wav`

---

# Building the Database

1. Sort existing assets into label directories
  `FileUtility.IntakeHandler.sort_intake()`
2. Ensure all canonical asset names are normalized
  `NormalizeApp.Interactive.homogenize_label_dirs`
3. Generate asset file survey json documents
  `AsynchSurvey.main_full()`
4. Build the database
  `CatalogApp.Build.run()`




FORMATS = [
    "composition",
    "construction kit",
    "loop",
    "one-shot",
    "song-starter",
    "stem"
]
INSTRUMENTS = {
    "drum": ["808", "perc", "snare", "kick", "tom", "hihat", "cymbal", "break", "snap", "clap", "crash"],
    "synth": ["pad", "lead"],
    "guitar": ["strum"],
    "bass": ["pluck"]
}