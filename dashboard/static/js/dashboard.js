// Dashboard Météo Global - JavaScript

const API_BASE = '/api';
let charts = {};
let updateInterval = null;

// Initialisation
document.addEventListener('DOMContentLoaded', () => {
    initTabs();
    initCitySelector();
    initCharts();
    startRealtimeUpdates();
    checkHealth();
    loadCities();
});

// Gestion des onglets
function initTabs() {
    const tabButtons = document.querySelectorAll('.tab-btn');
    const tabPanes = document.querySelectorAll('.tab-pane');

    tabButtons.forEach(btn => {
        btn.addEventListener('click', () => {
            const targetTab = btn.dataset.tab;

            // Désactiver tous les onglets
            tabButtons.forEach(b => b.classList.remove('active'));
            tabPanes.forEach(p => p.classList.remove('active'));

            // Activer l'onglet sélectionné
            btn.classList.add('active');
            document.getElementById(`${targetTab}-tab`).classList.add('active');

            // Charger les données de l'onglet
            loadTabData(targetTab);
        });
    });
}

// Sélecteur de ville
function initCitySelector() {
    const loadBtn = document.getElementById('load-city-btn');
    loadBtn.addEventListener('click', () => {
        const country = document.getElementById('country-select').value;
        const city = document.getElementById('city-select').value;
        if (country && city) {
            loadCityData(country, city);
        }
    });
}

async function loadCities() {
    try {
        const response = await fetch(`${API_BASE}/cities`);
        const data = await response.json();
        
        const countries = [...new Set(data.cities.map(c => c.country))];
        const countrySelect = document.getElementById('country-select');
        countrySelect.innerHTML = '<option value="">Sélectionnez un pays</option>';
        
        countries.forEach(country => {
            const option = document.createElement('option');
            option.value = country;
            option.textContent = country;
            countrySelect.appendChild(option);
        });

        countrySelect.addEventListener('change', () => {
            const selectedCountry = countrySelect.value;
            const cities = data.cities.filter(c => c.country === selectedCountry);
            const citySelect = document.getElementById('city-select');
            citySelect.innerHTML = '<option value="">Sélectionnez une ville</option>';
            
            cities.forEach(city => {
                const option = document.createElement('option');
                option.value = city.city;
                option.textContent = city.city;
                citySelect.appendChild(option);
            });
        });
    } catch (error) {
        console.error('Erreur chargement villes:', error);
    }
}

// Initialisation des graphiques
function initCharts() {
    // Graphique température temps réel
    charts.realtimeTemp = new Chart(document.getElementById('realtime-temp-chart'), {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                label: 'Température (°C)',
                data: [],
                borderColor: 'rgb(239, 68, 68)',
                backgroundColor: 'rgba(239, 68, 68, 0.1)',
                tension: 0.4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            scales: {
                y: { beginAtZero: false }
            }
        }
    });

    // Graphique vent temps réel
    charts.realtimeWind = new Chart(document.getElementById('realtime-wind-chart'), {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                label: 'Vitesse du Vent (m/s)',
                data: [],
                borderColor: 'rgb(59, 130, 246)',
                backgroundColor: 'rgba(59, 130, 246, 0.1)',
                tension: 0.4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            scales: {
                y: { beginAtZero: true }
            }
        }
    });

    // Graphique anomalies (pie)
    charts.anomaliesPie = new Chart(document.getElementById('anomalies-pie-chart'), {
        type: 'pie',
        data: {
            labels: [],
            datasets: [{
                data: [],
                backgroundColor: [
                    'rgba(239, 68, 68, 0.8)',
                    'rgba(59, 130, 246, 0.8)',
                    'rgba(16, 185, 129, 0.8)',
                    'rgba(245, 158, 11, 0.8)'
                ]
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true
        }
    });

    // Graphique alertes
    charts.alerts = new Chart(document.getElementById('alerts-chart'), {
        type: 'bar',
        data: {
            labels: [],
            datasets: [
                {
                    label: 'Vent Level 1',
                    data: [],
                    backgroundColor: 'rgba(245, 158, 11, 0.8)'
                },
                {
                    label: 'Vent Level 2',
                    data: [],
                    backgroundColor: 'rgba(239, 68, 68, 0.8)'
                },
                {
                    label: 'Chaleur Level 1',
                    data: [],
                    backgroundColor: 'rgba(59, 130, 246, 0.8)'
                },
                {
                    label: 'Chaleur Level 2',
                    data: [],
                    backgroundColor: 'rgba(220, 38, 127, 0.8)'
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            scales: {
                y: { beginAtZero: true }
            }
        }
    });
}

// Vérification de santé
async function checkHealth() {
    try {
        const response = await fetch(`${API_BASE}/health`);
        const data = await response.json();
        
        const statusIndicator = document.getElementById('connection-status');
        if (data.kafka_connected) {
            statusIndicator.classList.add('connected');
            statusIndicator.classList.remove('disconnected');
        } else {
            statusIndicator.classList.add('disconnected');
            statusIndicator.classList.remove('connected');
        }
        
        document.getElementById('last-update').textContent = 
            `Dernière mise à jour: ${new Date().toLocaleTimeString()}`;
    } catch (error) {
        console.error('Erreur health check:', error);
        document.getElementById('connection-status').classList.add('disconnected');
    }
}

// Mises à jour en temps réel
function startRealtimeUpdates() {
    updateRealtimeData();
    updateInterval = setInterval(() => {
        updateRealtimeData();
        checkHealth();
    }, 5000); // Toutes les 5 secondes
}

async function updateRealtimeData() {
    try {
        // Mettre à jour les statistiques
        const statsResponse = await fetch(`${API_BASE}/stats/summary`);
        const stats = await statsResponse.json();
        
        document.getElementById('realtime-count').textContent = stats.realtime_messages || 0;
        document.getElementById('anomalies-count').textContent = stats.anomalies_detected || 0;
        document.getElementById('aggregates-count').textContent = stats.aggregates_count || 0;

        // Mettre à jour les données temps réel
        const weatherResponse = await fetch(`${API_BASE}/realtime/weather?limit=20`);
        const weatherData = await weatherResponse.json();
        
        if (weatherData.data && weatherData.data.length > 0) {
            updateRealtimeCharts(weatherData.data);
            updateRealtimeTable(weatherData.data);
        }

        // Mettre à jour les anomalies
        const anomaliesResponse = await fetch(`${API_BASE}/realtime/anomalies?limit=10`);
        const anomaliesData = await anomaliesResponse.json();
        
        if (anomaliesData.data && anomaliesData.data.length > 0) {
            updateAnomaliesDisplay(anomaliesData.data);
        }
    } catch (error) {
        console.error('Erreur mise à jour temps réel:', error);
    }
}

function updateRealtimeCharts(data) {
    const labels = data.map(d => {
        const date = new Date(d.event_time || d._kafka_timestamp);
        return date.toLocaleTimeString();
    }).reverse();
    
    const temps = data.map(d => d.temperature).reverse();
    const winds = data.map(d => d.windspeed).reverse();

    charts.realtimeTemp.data.labels = labels;
    charts.realtimeTemp.data.datasets[0].data = temps;
    charts.realtimeTemp.update('none');

    charts.realtimeWind.data.labels = labels;
    charts.realtimeWind.data.datasets[0].data = winds;
    charts.realtimeWind.update('none');
}

function updateRealtimeTable(data) {
    const tbody = document.querySelector('#realtime-table tbody');
    tbody.innerHTML = '';
    
    data.slice(0, 10).forEach(item => {
        const row = document.createElement('tr');
        const date = new Date(item.event_time || item._kafka_timestamp);
        
        row.innerHTML = `
            <td>${item.city || 'N/A'}</td>
            <td>${item.temperature?.toFixed(1) || 'N/A'}°C</td>
            <td>${item.windspeed?.toFixed(1) || 'N/A'} m/s</td>
            <td>
                ${item.wind_alert_level !== 'level_0' ? `🌪️ ${item.wind_alert_level}` : ''}
                ${item.heat_alert_level !== 'level_0' ? `🔥 ${item.heat_alert_level}` : ''}
            </td>
            <td>${date.toLocaleString()}</td>
        `;
        tbody.appendChild(row);
    });
}

function updateAnomaliesDisplay(data) {
    const types = {};
    data.forEach(anomaly => {
        const type = anomaly.anomaly_type || 'unknown';
        types[type] = (types[type] || 0) + 1;
    });

    charts.anomaliesPie.data.labels = Object.keys(types);
    charts.anomaliesPie.data.datasets[0].data = Object.values(types);
    charts.anomaliesPie.update('none');

    // Mettre à jour le tableau
    const tbody = document.querySelector('#anomalies-table tbody');
    tbody.innerHTML = '';
    
    data.slice(0, 10).forEach(anomaly => {
        const row = document.createElement('tr');
        const date = new Date(anomaly.event_time);
        const deviation = anomaly.observed_value - anomaly.expected_value;
        
        row.innerHTML = `
            <td>${date.toLocaleString()}</td>
            <td>${anomaly.anomaly_type}</td>
            <td>${anomaly.variable}</td>
            <td>${anomaly.observed_value?.toFixed(2) || 'N/A'}</td>
            <td>${anomaly.expected_value?.toFixed(2) || 'N/A'}</td>
            <td>${deviation?.toFixed(2) || 'N/A'}</td>
        `;
        tbody.appendChild(row);
    });
}

// Chargement des données par onglet
async function loadTabData(tab) {
    const country = document.getElementById('country-select').value;
    const city = document.getElementById('city-select').value;
    
    if (!country || !city) {
        return;
    }

    switch(tab) {
        case 'historical':
            await loadHistoricalData(country, city);
            break;
        case 'seasonal':
            await loadSeasonalProfile(country, city);
            break;
        case 'anomalies':
            await loadAnomalies(country, city);
            break;
        case 'records':
            await loadRecords(country, city);
            break;
        case 'alerts':
            await loadAlerts(country, city);
            break;
    }
}

async function loadCityData(country, city) {
    // Charger toutes les données pour la ville sélectionnée
    await loadHistoricalData(country, city);
    await loadSeasonalProfile(country, city);
    await loadAnomalies(country, city);
    await loadRecords(country, city);
    await loadAlerts(country, city);
}

// Données historiques
async function loadHistoricalData(country, city) {
    try {
        const response = await fetch(`${API_BASE}/historical/${country}/${city}`);
        const data = await response.json();
        
        if (data.data && data.data.hourly) {
            const hourly = data.data.hourly;
            const times = hourly.time || [];
            const temps = hourly.temperature_2m || [];
            const winds = hourly.windspeed_10m || [];

            // Créer les graphiques si nécessaire
            if (!charts.historicalTemp) {
                charts.historicalTemp = new Chart(document.getElementById('historical-temp-chart'), {
                    type: 'line',
                    data: { labels: [], datasets: [] },
                    options: { responsive: true, maintainAspectRatio: true }
                });
            }

            if (!charts.historicalWind) {
                charts.historicalWind = new Chart(document.getElementById('historical-wind-chart'), {
                    type: 'line',
                    data: { labels: [], datasets: [] },
                    options: { responsive: true, maintainAspectRatio: true }
                });
            }

            // Mettre à jour les graphiques (échantillonnage pour performance)
            const step = Math.max(1, Math.floor(times.length / 1000));
            const sampledTimes = times.filter((_, i) => i % step === 0);
            const sampledTemps = temps.filter((_, i) => i % step === 0);
            const sampledWinds = winds.filter((_, i) => i % step === 0);

            charts.historicalTemp.data.labels = sampledTimes.map(t => new Date(t).toLocaleDateString());
            charts.historicalTemp.data.datasets = [{
                label: 'Température (°C)',
                data: sampledTemps,
                borderColor: 'rgb(239, 68, 68)',
                backgroundColor: 'rgba(239, 68, 68, 0.1)'
            }];
            charts.historicalTemp.update();

            charts.historicalWind.data.labels = sampledTimes.map(t => new Date(t).toLocaleDateString());
            charts.historicalWind.data.datasets = [{
                label: 'Vitesse du Vent (m/s)',
                data: sampledWinds,
                borderColor: 'rgb(59, 130, 246)',
                backgroundColor: 'rgba(59, 130, 246, 0.1)'
            }];
            charts.historicalWind.update();
        }
    } catch (error) {
        console.error('Erreur chargement historique:', error);
    }
}

// Profil saisonnier
async function loadSeasonalProfile(country, city) {
    try {
        const response = await fetch(`${API_BASE}/seasonal-profile/${country}/${city}`);
        const data = await response.json();
        
        if (data.profile && data.profile.monthly_profiles) {
            const profiles = data.profile.monthly_profiles;
            const months = ['Jan', 'Fév', 'Mar', 'Avr', 'Mai', 'Jun', 'Jul', 'Aoû', 'Sep', 'Oct', 'Nov', 'Déc'];
            
            const temps = profiles.map(p => p.avg_temperature);
            const winds = profiles.map(p => p.avg_windspeed);
            const alerts = profiles.map(p => p.alert_probability);

            // Créer les graphiques si nécessaire
            if (!charts.seasonalTemp) {
                charts.seasonalTemp = new Chart(document.getElementById('seasonal-temp-chart'), {
                    type: 'bar',
                    data: { labels: months, datasets: [] },
                    options: { responsive: true, maintainAspectRatio: true }
                });
            }

            charts.seasonalTemp.data.datasets = [{
                label: 'Température Moyenne (°C)',
                data: temps,
                backgroundColor: 'rgba(239, 68, 68, 0.8)'
            }];
            charts.seasonalTemp.update();

            if (!charts.seasonalWind) {
                charts.seasonalWind = new Chart(document.getElementById('seasonal-wind-chart'), {
                    type: 'bar',
                    data: { labels: months, datasets: [] },
                    options: { responsive: true, maintainAspectRatio: true }
                });
            }

            charts.seasonalWind.data.datasets = [{
                label: 'Vitesse du Vent Moyenne (m/s)',
                data: winds,
                backgroundColor: 'rgba(59, 130, 246, 0.8)'
            }];
            charts.seasonalWind.update();

            if (!charts.seasonalAlert) {
                charts.seasonalAlert = new Chart(document.getElementById('seasonal-alert-chart'), {
                    type: 'line',
                    data: { labels: months, datasets: [] },
                    options: { responsive: true, maintainAspectRatio: true }
                });
            }

            charts.seasonalAlert.data.datasets = [{
                label: 'Probabilité d\'Alerte (%)',
                data: alerts,
                borderColor: 'rgb(245, 158, 11)',
                backgroundColor: 'rgba(245, 158, 11, 0.1)'
            }];
            charts.seasonalAlert.update();

            // Statistiques
            const maxTemp = Math.max(...temps);
            const minTemp = Math.min(...temps);
            const maxWind = Math.max(...winds);
            
            document.getElementById('hottest-month').textContent = 
                months[temps.indexOf(maxTemp)];
            document.getElementById('coldest-month').textContent = 
                months[temps.indexOf(minTemp)];
            document.getElementById('windiest-month').textContent = 
                months[winds.indexOf(maxWind)];
        }
    } catch (error) {
        console.error('Erreur chargement profil saisonnier:', error);
    }
}

// Anomalies
async function loadAnomalies(country, city) {
    try {
        const response = await fetch(`${API_BASE}/anomalies/${country}/${city}`);
        const data = await response.json();
        
        document.getElementById('total-anomalies').textContent = data.count || 0;
        
        const types = {};
        data.anomalies.forEach(a => {
            types[a.anomaly_type] = (types[a.anomaly_type] || 0) + 1;
        });
        
        document.getElementById('heat-waves').textContent = types.heat_wave || 0;
        document.getElementById('cold-spells').textContent = types.cold_spell || 0;
        document.getElementById('wind-storms').textContent = types.wind_storm || 0;

        // Mettre à jour le graphique
        charts.anomaliesPie.data.labels = Object.keys(types);
        charts.anomaliesPie.data.datasets[0].data = Object.values(types);
        charts.anomaliesPie.update();
    } catch (error) {
        console.error('Erreur chargement anomalies:', error);
    }
}

// Records
async function loadRecords(country, city) {
    try {
        const response = await fetch(`${API_BASE}/records/${country}/${city}`);
        const data = await response.json();
        
        if (data.records && data.records.length > 0) {
            const records = data.records[0]; // Prendre le premier record
            
            if (records.hottest_day) {
                document.getElementById('hottest-day').textContent = 
                    `${records.hottest_day.temperature.toFixed(1)}°C`;
                document.getElementById('hottest-date').textContent = 
                    new Date(records.hottest_day.date).toLocaleDateString();
            }
            
            if (records.coldest_day) {
                document.getElementById('coldest-day').textContent = 
                    `${records.coldest_day.temperature.toFixed(1)}°C`;
                document.getElementById('coldest-date').textContent = 
                    new Date(records.coldest_day.date).toLocaleDateString();
            }
            
            if (records.strongest_wind) {
                document.getElementById('strongest-wind').textContent = 
                    `${records.strongest_wind.windspeed.toFixed(1)} m/s`;
                document.getElementById('wind-date').textContent = 
                    new Date(records.strongest_wind.date).toLocaleDateString();
            }
            
            if (records.rainiest_day) {
                document.getElementById('rainiest-day').textContent = 
                    `${records.rainiest_day.precipitation.toFixed(1)} mm`;
                document.getElementById('rainiest-date').textContent = 
                    new Date(records.rainiest_day.date).toLocaleDateString();
            }
        }
    } catch (error) {
        console.error('Erreur chargement records:', error);
    }
}

// Alertes
async function loadAlerts(country, city) {
    try {
        const response = await fetch(`${API_BASE}/alerts/${country}/${city}`);
        const data = await response.json();
        
        document.getElementById('total-alerts').textContent = data.count || 0;
        
        const wind1 = data.alerts.filter(a => a.wind_alert_level === 'level_1').length;
        const wind2 = data.alerts.filter(a => a.wind_alert_level === 'level_2').length;
        const heat1 = data.alerts.filter(a => a.heat_alert_level === 'level_1').length;
        const heat2 = data.alerts.filter(a => a.heat_alert_level === 'level_2').length;
        
        document.getElementById('wind-alerts-1').textContent = wind1;
        document.getElementById('wind-alerts-2').textContent = wind2;
        document.getElementById('heat-alerts-1').textContent = heat1;
        document.getElementById('heat-alerts-2').textContent = heat2;

        // Mettre à jour le tableau
        const tbody = document.querySelector('#alerts-table tbody');
        tbody.innerHTML = '';
        
        data.alerts.slice(0, 20).forEach(alert => {
            const row = document.createElement('tr');
            const date = new Date(alert.event_time);
            
            row.innerHTML = `
                <td>${date.toLocaleString()}</td>
                <td>${alert.city || city}</td>
                <td>
                    ${alert.wind_alert_level !== 'level_0' ? 'Vent' : ''}
                    ${alert.heat_alert_level !== 'level_0' ? 'Chaleur' : ''}
                </td>
                <td>
                    ${alert.wind_alert_level !== 'level_0' ? alert.wind_alert_level : ''}
                    ${alert.heat_alert_level !== 'level_0' ? alert.heat_alert_level : ''}
                </td>
                <td>${alert.temperature?.toFixed(1) || 'N/A'}°C</td>
                <td>${alert.windspeed?.toFixed(1) || 'N/A'} m/s</td>
            `;
            tbody.appendChild(row);
        });
    } catch (error) {
        console.error('Erreur chargement alertes:', error);
    }
}
