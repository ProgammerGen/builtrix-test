import express from 'express'
import cors from 'cors'
import sqlite3 from 'sqlite3'
import { fileURLToPath } from 'url'
import { dirname, join } from 'path'
import fs from 'fs'
import csv from 'csv-parser'

const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)

const app = express()
const PORT = process.env.PORT || 3001

// Middleware
app.use(cors())
app.use(express.json())

const dbPath = join(__dirname, 'energy_data.db')
const db = new sqlite3.Database(dbPath)

function initializeDatabase() {
  return new Promise((resolve, reject) => {
    db.serialize(() => {
      db.run(`
        CREATE TABLE IF NOT EXISTS metadata (
          cpe TEXT PRIMARY KEY,
          lat REAL,
          lon REAL,
          totalarea REAL,
          name TEXT,
          fulladdress TEXT
        )
      `)

      db.run(`
        CREATE TABLE IF NOT EXISTS smart_meter_data (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          cpe TEXT,
          timestamp TEXT,
          active_energy REAL,
          FOREIGN KEY (cpe) REFERENCES metadata (cpe)
        )
      `)

      db.run(`
        CREATE TABLE IF NOT EXISTS energy_breakdown (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          timestamp TEXT,
          renewable_biomass REAL,
          renewable_hydro REAL,
          renewable_solar REAL,
          renewable_wind REAL,
          renewable_geothermal REAL,
          renewable_otherrenewable REAL,
          renewable REAL,
          nonrenewable_coal REAL,
          nonrenewable_gas REAL,
          nonrenewable_nuclear REAL,
          nonrenewable_oil REAL,
          nonrenewable REAL,
          hydropumpedstorage REAL,
          unknown REAL
        )
      `)

      db.run('CREATE INDEX IF NOT EXISTS idx_smart_meter_cpe ON smart_meter_data(cpe)')
      db.run('CREATE INDEX IF NOT EXISTS idx_smart_meter_timestamp ON smart_meter_data(timestamp)')
      db.run('CREATE INDEX IF NOT EXISTS idx_breakdown_timestamp ON energy_breakdown(timestamp)')

      resolve()
    })
  })
}

async function loadCSVData() {
  try {
    // Clear existing data first
    console.log('Clearing existing data...')
    db.run('DELETE FROM smart_meter_data')
    db.run('DELETE FROM energy_breakdown')
    db.run('DELETE FROM metadata')

    // Load metadata
    const metadataPath = join(__dirname, 'data', 'metadata.csv')
    if (fs.existsSync(metadataPath)) {
      const metadata = []
      let isFirstRow = true
      let headers = []

      fs.createReadStream(metadataPath)
        .pipe(csv({
          stripBOM: true,
          headers: false
        }))
        .on('data', (row) => {
          if (isFirstRow) {
            headers = Object.values(row).map((val) => val.toString().replace(/^\uFEFF/, '').trim())
            console.log('Headers detected:', headers)
            isFirstRow = false
          } else {
            const dataRow = {}

            Object.keys(row).forEach((key, index) => {
              const header = headers[index]
              dataRow[header] = row[key]
            })

            metadata.push(dataRow)
          }
        })
        .on('end', () => {
          console.log(`Parsed ${metadata.length} metadata rows`)
          if (metadata.length > 0) {
            console.log('Sample row:', metadata[0])
          }

          db.serialize(() => {
            const stmt = db.prepare('INSERT OR REPLACE INTO metadata (cpe, lat, lon, totalarea, name, fulladdress) VALUES (?, ?, ?, ?, ?, ?)')
            let insertedCount = 0

            // Insert all rows
            metadata.forEach(row => {
              if (row.cpe && row.cpe.trim()) {
                stmt.run(
                  row.cpe.trim(),
                  parseFloat(row.lat),
                  parseFloat(row.lon),
                  parseFloat(row.totalarea),
                  row.name,
                  row.fulladdress
                )
                insertedCount++
              } else {
                console.warn('Skipping row with null/empty cpe:', row)
              }
            })

            stmt.finalize(() => {
              console.log(`Metadata loaded successfully: ${insertedCount} records inserted`)
            })
          })
        })
    }


    const smartMeterPath = join(__dirname, 'data', 'smart_meter.csv')
    if (fs.existsSync(smartMeterPath)) {
      let count = 0
      const maxRecords = 10000
      fs.createReadStream(smartMeterPath)
        .pipe(csv({
          stripBOM: true
        }))
        .on('data', (row) => {
          if (count < maxRecords && row.cpe) {
            const stmt = db.prepare('INSERT INTO smart_meter_data (cpe, timestamp, active_energy) VALUES (?, ?, ?)')
            stmt.run(row.cpe, row.timestamp, parseFloat(row.active_energy))
            stmt.finalize()
            count++
          }
        })
        .on('end', () => {
          console.log(`Smart meter data loaded: ${count} records`)
        })
    }

    const breakdownPath = join(__dirname, 'data', 'energy_source_breakdown.csv')
    if (fs.existsSync(breakdownPath)) {
      let count = 0
      const maxRecords = 1000
      fs.createReadStream(breakdownPath)
        .pipe(csv({
          stripBOM: true
        }))
        .on('data', (row) => {
          if (count < maxRecords) {
            const stmt = db.prepare(`
              INSERT INTO energy_breakdown (
                timestamp, renewable_biomass, renewable_hydro, renewable_solar, renewable_wind,
                renewable_geothermal, renewable_otherrenewable, renewable, nonrenewable_coal,
                nonrenewable_gas, nonrenewable_nuclear, nonrenewable_oil, nonrenewable,
                hydropumpedstorage, unknown
              ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `)
            stmt.run(
              row.timestamp,
              parseFloat(row.renewable_biomass),
              parseFloat(row.renewable_hydro),
              parseFloat(row.renewable_solar),
              parseFloat(row.renewable_wind),
              parseFloat(row.renewable_geothermal),
              parseFloat(row.renewable_otherrenewable),
              parseFloat(row.renewable),
              parseFloat(row.nonrenewable_coal),
              parseFloat(row.nonrenewable_gas),
              parseFloat(row.nonrenewable_nuclear),
              parseFloat(row.nonrenewable_oil),
              parseFloat(row.nonrenewable),
              parseFloat(row.hydropumpedstorage),
              parseFloat(row.unknown)
            )
            stmt.finalize()
            count++
          }
        })
        .on('end', () => {
          console.log(`Energy breakdown data loaded: ${count} records`)
        })
    }
  } catch (error) {
    console.error('Error loading CSV data:', error)
  }
}

app.get('/api/buildings', (req, res) => {
  const query = `
    SELECT 
      m.cpe,
      m.lat,
      m.lon,
      m.totalarea,
      m.name,
      m.fulladdress,
      COALESCE(SUM(smd.active_energy), 0) AS annual_energy
    FROM metadata m
    LEFT JOIN smart_meter_data smd ON m.cpe = smd.cpe
    GROUP BY m.cpe, m.name, m.lat, m.lon, m.totalarea, m.fulladdress
    ORDER BY annual_energy DESC
  `

  db.all(query, (err, rows) => {
    if (err) {
      res.status(500).json({ error: err.message })
      return
    }
    console.log(`Retrieved ${rows.length} buildings`)
    res.json(rows)
  })
})

app.get('/api/energy/:cpe/:period', (req, res) => {
  const { cpe, period } = req.params
  const { year, month, day } = req.query

  let query = ''
  let params = [cpe]

  switch (period) {
    case 'monthly':
      query = `
        SELECT 
          strftime('%Y-%m', timestamp) as month,
          SUM(active_energy) as total_energy
        FROM smart_meter_data 
        WHERE cpe = ? AND strftime('%Y', timestamp) = ?
        GROUP BY strftime('%Y-%m', timestamp)
        ORDER BY month
      `
      params.push(year)
      break
    case 'daily':
      query = `
        SELECT 
          strftime('%Y-%m-%d', timestamp) as day,
          SUM(active_energy) as total_energy
        FROM smart_meter_data 
        WHERE cpe = ? AND strftime('%Y-%m', timestamp) = ?
        GROUP BY strftime('%Y-%m-%d', timestamp)
        ORDER BY day
      `
      params.push(month || `${year}-01`)
      break
    case 'hourly':
      query = `
        SELECT 
          strftime('%H:00', timestamp) as hour,
          SUM(active_energy) as total_energy
        FROM smart_meter_data 
        WHERE cpe = ? AND strftime('%Y-%m-%d', timestamp) = ?
        GROUP BY strftime('%H', timestamp)
        ORDER BY hour
      `
      params.push(day || `${year}-01-01`)
      break
    default:
      res.status(400).json({ error: 'Invalid period' })
      return
  }

  db.all(query, params, (err, rows) => {
    if (err) {
      res.status(500).json({ error: err.message })
      return
    }
    res.json(rows)
  })
})

app.get('/api/energy-breakdown', (req, res) => {
  const { timestamp } = req.query

  let query = 'SELECT * FROM energy_breakdown'
  let params = []

  if (timestamp) {
    query += ' WHERE timestamp LIKE ?'
    params.push(`${timestamp}%`)
  }

  query += ' ORDER BY timestamp LIMIT 24'

  db.all(query, params, (err, rows) => {
    if (err) {
      res.status(500).json({ error: err.message })
      return
    }
    res.json(rows)
  })
})

app.get('/api/buildings-energy', (req, res) => {
  const { year } = req.query

  const query = `
    SELECT 
      m.cpe,
      m.name,
      m.lat,
      m.lon,
      m.totalarea,
      SUM(smd.active_energy) as annual_energy
    FROM metadata m
    LEFT JOIN smart_meter_data smd ON m.cpe = smd.cpe
    WHERE strftime('%Y', smd.timestamp) = ?
    GROUP BY m.cpe, m.name, m.lat, m.lon, m.totalarea
    ORDER BY annual_energy DESC
  `

  db.all(query, [year || '2021'], (err, rows) => {
    if (err) {
      res.status(500).json({ error: err.message })
      return
    }
    res.json(rows)
  })
})

// Debug endpoint to check database contents
app.get('/api/debug/metadata', (req, res) => {
  db.all('SELECT * FROM metadata LIMIT 5', (err, rows) => {
    if (err) {
      res.status(500).json({ error: err.message })
      return
    }
    res.json({
      count: rows.length,
      sample: rows,
      columns: rows.length > 0 ? Object.keys(rows[0]) : []
    })
  })
})

async function startServer() {
  try {
    await initializeDatabase()
    await loadCSVData()

    app.listen(PORT, () => {
      console.log(`Server running on port ${PORT}`)
      console.log(`Database initialized at: ${dbPath}`)
    })
  } catch (error) {
    console.error('Failed to start server:', error)
  }
}

startServer()
