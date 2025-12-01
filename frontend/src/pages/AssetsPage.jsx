import React, { useState, useEffect, useRef } from 'react';
import { io } from 'socket.io-client';
import {
  Box,
  Typography,
  Card,
  CardContent,
  Grid,
  Chip,
  Button,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  TextField,
  InputAdornment,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Tabs,
  Tab,
  Divider,
  Alert,
  CircularProgress,
  Pagination,
  Stack,
  IconButton,
  Badge,
  Tooltip,
  Snackbar,
} from '@mui/material';
import {
  Search,
  Refresh,
  DataObject,
  FilterList,
  Visibility,
  Download,
  Warning,
  CheckCircle,
  Close,
  Notifications,
  AddCircle,
  Delete,
} from '@mui/icons-material';
import AssetsChatbot from '../components/AssetsChatbot';

const AssetsPage = () => {
  const [assets, setAssets] = useState([]);
  const [loading, setLoading] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');
  const [typeFilter, setTypeFilter] = useState('');
  const [catalogFilter, setCatalogFilter] = useState('');
  const [selectedAsset, setSelectedAsset] = useState(null);
  const [detailsDialogOpen, setDetailsDialogOpen] = useState(false);
  const [activeTab, setActiveTab] = useState(0);
  const [classification, setClassification] = useState('internal');
  const [sensitivityLevel, setSensitivityLevel] = useState('medium');
  const [originalClassification, setOriginalClassification] = useState('internal');
  const [originalSensitivityLevel, setOriginalSensitivityLevel] = useState('medium');
  const [savingMetadata, setSavingMetadata] = useState(false);
  
  // Pagination state
  const [currentPage, setCurrentPage] = useState(0);
  const [pageSize, setPageSize] = useState(50);
  const [totalAssets, setTotalAssets] = useState(0);
  const [totalPages, setTotalPages] = useState(0);
  const [allAssets, setAllAssets] = useState([]); // For filters
  const [bigqueryTotal, setBigqueryTotal] = useState(0);
  const [starburstTotal, setStarburstTotal] = useState(0);
  const [s3Total, setS3Total] = useState(0);
  
  // Pending assets state
  const [pendingAssets, setPendingAssets] = useState([]);
  const [pendingDialogOpen, setPendingDialogOpen] = useState(false);
  const [noNotificationsSnackbar, setNoNotificationsSnackbar] = useState(false);
  const [discoveryLoading, setDiscoveryLoading] = useState(false);
  const socketRef = useRef(null);

  // Socket.IO connection for real-time notifications
  useEffect(() => {
    let socket = null;
    
    try {
      // Only initialize Socket.IO if not already connected
      if (!socketRef.current || socketRef.current.disconnected) {
        console.log('🔧 Initializing Socket.IO connection to /assets namespace...');
        socketRef.current = io('http://localhost:8099/assets', {
          // Start with polling only to avoid WebSocket frame header issues
          transports: ['polling'],  // Use polling only - more reliable
          reconnection: true,  // Enable auto-reconnect
          reconnectionDelay: 1000,
          reconnectionDelayMax: 5000,
          reconnectionAttempts: 5,
          timeout: 20000,
          autoConnect: true,
          forceNew: false,  // Don't force new connection
          upgrade: false,  // Disable automatic upgrade to websocket to avoid frame header errors
          rememberUpgrade: false,
          withCredentials: false
        });
        socket = socketRef.current;
      } else {
        console.log('✅ Socket already connected, reusing existing connection');
        socket = socketRef.current;
      }

      // Socket.IO connection event listeners
      socket.on('connect', () => {
        console.log('✅ Socket.IO Connected to /assets namespace! ID:', socket.id);
      });

      socket.on('disconnect', (reason) => {
        console.log('❌ Socket.IO Disconnected from /assets namespace. Reason:', reason);
      });

      socket.on('connect_error', (error) => {
        // Don't spam console with connection errors - they're expected during initial connection attempts
        // Socket.io will automatically retry, and we have polling as fallback
        if (error.message && !error.message.includes('xhr poll error') && !error.message.includes('timeout')) {
          console.warn('⚠️ Socket.IO Connection Error (will retry):', error.message);
        }
      });

      // Listen for pending asset notifications
      socket.on('pending_asset_created', (data) => {
        console.log('📢 Received pending asset notification:', data);
        // Refresh pending assets immediately
        fetchPendingAssets();
      });
    } catch (error) {
      console.error('❌ Error setting up Socket.IO connection:', error);
    }

    return () => {
      if (socketRef.current) {
        try {
          socketRef.current.off('connect');
          socketRef.current.off('disconnect');
          socketRef.current.off('connect_error');
          socketRef.current.off('pending_asset_created');
          socketRef.current.disconnect();
        } catch (e) {
          // Ignore cleanup errors
        }
        socketRef.current = null;
      }
    };
  }, []); // Empty deps - socket connection should only be set up once

  useEffect(() => {
    fetchAssets();
    fetchPendingAssets();
    // Poll for pending assets every 30 seconds (reduced frequency since we have socket.io)
    const pendingInterval = setInterval(fetchPendingAssets, 30000);
    return () => clearInterval(pendingInterval);
  }, [currentPage, pageSize, searchTerm, typeFilter, catalogFilter]);

  // Also fetch totals when page changes (to ensure counts are accurate)
  useEffect(() => {
    if (!searchTerm && !typeFilter && !catalogFilter) {
      fetchTotals();
    }
  }, [currentPage]);

  // Fetch totals separately (only when no filters are applied)
  useEffect(() => {
    if (!searchTerm && !typeFilter && !catalogFilter) {
      fetchTotals();
    }
  }, [searchTerm, typeFilter, catalogFilter]);

  const fetchTotals = async () => {
    try {
      // Backend limits size to 100; page through results to compute totals safely
      let page = 0;
      const size = 100;
      let fetchedAll = false;
      let bigqueryCount = 0;
      let starburstCount = 0;
      let s3Count = 0;
      const aggregatedAssets = [];
      let totalFromAPI = 0;

      while (!fetchedAll) {
        const resp = await fetch(`http://localhost:8099/api/assets?page=${page}&size=${size}`);
        if (!resp.ok) {
          // Stop early on HTTP errors
          throw new Error(`HTTP ${resp.status}`);
        }
        const data = await resp.json();
        const assetsPage = Array.isArray(data.assets) ? data.assets : [];
        
        // Get total from first page's pagination
        if (page === 0 && data?.pagination?.total) {
          totalFromAPI = data.pagination.total;
          setTotalAssets(totalFromAPI); // Update total assets count immediately
        }

        // Accumulate counts and assets for filters
        for (const asset of assetsPage) {
          const id = asset?.connector_id || '';
          if (id.startsWith('bq_')) bigqueryCount += 1;
          else if (id.startsWith('starburst_')) starburstCount += 1;
          else if (id.startsWith('s3_')) s3Count += 1;
        }
        aggregatedAssets.push(...assetsPage);

        // Determine if there are more pages
        const hasNext = Boolean(data?.pagination?.has_next);
        if (hasNext) {
          page += 1;
        } else {
          fetchedAll = true;
        }
        
        // Safety check: if we've fetched more than the API says exists, stop
        if (totalFromAPI > 0 && aggregatedAssets.length >= totalFromAPI) {
          fetchedAll = true;
        }
      }

      // Use API total if available, otherwise use counted total
      const finalTotal = totalFromAPI || aggregatedAssets.length;
      console.log('✅ Totals fetched:', { 
        bigqueryCount, 
        starburstCount, 
        s3Count, 
        totalAssets: finalTotal,
        assetsFetched: aggregatedAssets.length,
        apiTotal: totalFromAPI
      });
      // Update all card counts
      setTotalAssets(finalTotal); // Ensure totalAssets is updated
      setBigqueryTotal(bigqueryCount);
      setStarburstTotal(starburstCount);
      setS3Total(s3Count);
      setAllAssets(aggregatedAssets); // For filter dropdowns
    } catch (error) {
      console.error('Error fetching totals:', error);
      // Fallback to zeros to avoid UI crashes
      setBigqueryTotal(0);
      setStarburstTotal(0);
      setS3Total(0);
      setAllAssets([]);
    }
  };

  const fetchAssets = async (pageOverride = null) => {
    try {
      setLoading(true);
      
      // Use pageOverride if provided, otherwise use currentPage state
      const pageToUse = pageOverride !== null ? pageOverride : currentPage;
      
      // Build query parameters
      const params = new URLSearchParams({
        page: pageToUse.toString(),
        size: pageSize.toString(),
      });
      
      if (searchTerm) params.append('search', searchTerm);
      if (catalogFilter) params.append('catalog', catalogFilter);
      if (typeFilter) params.append('asset_type', typeFilter);
      
      const response = await fetch(`http://localhost:8099/api/assets?${params}`);
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      const data = await response.json();
      const assetsList = Array.isArray(data.assets) ? data.assets : [];
      const pagination = data?.pagination || { total: 0, total_pages: 0 };
      
      // Sort assets by sort_order (if available) or discovered_at (newest first) to ensure newly added appear at top
      assetsList.sort((a, b) => {
        // Prefer sort_order if available (higher = newer = top)
        const sortOrderA = a.sort_order !== null && a.sort_order !== undefined ? a.sort_order : -1;
        const sortOrderB = b.sort_order !== null && b.sort_order !== undefined ? b.sort_order : -1;
        
        if (sortOrderA !== -1 || sortOrderB !== -1) {
          // At least one has sort_order, use it for sorting
          return sortOrderB - sortOrderA; // Descending order (higher sort_order first)
        }
        
        // Fallback to discovered_at if no sort_order
        const dateA = a.discovered_at ? new Date(a.discovered_at) : new Date(0);
        const dateB = b.discovered_at ? new Date(b.discovered_at) : new Date(0);
        return dateB.getTime() - dateA.getTime(); // Descending order (newest first)
      });
      
      setAssets(assetsList);
      setTotalAssets(pagination.total || assetsList.length);
      setTotalPages(pagination.total_pages || 0);
    } catch (error) {
      console.error('Error fetching assets:', error);
    } finally {
      setLoading(false);
    }
  };

  // Get unique types and catalogs for filter dropdowns
  const uniqueTypes = [...new Set(allAssets.map(asset => asset.type))];
  const uniqueCatalogs = [...new Set(allAssets.map(asset => asset.catalog))];
  
  // Count assets from BigQuery data source (use stored totals)
  const bigqueryAssets = bigqueryTotal;
  
  // Count assets from Starburst Galaxy data source (use stored totals)
  const starburstAssets = starburstTotal;
  
  // Count assets from Amazon S3 data source (use stored totals)
  const s3Assets = s3Total;

  const getDataSource = (connectorId) => {
    if (!connectorId) return 'Unknown';
    if (connectorId.startsWith('bq_')) return 'BigQuery';
    if (connectorId.startsWith('starburst_')) return 'Starburst Galaxy';
    if (connectorId.startsWith('s3_')) return 'Amazon S3';
    if (connectorId.startsWith('gcs_')) return 'Google Cloud Storage';
    if (connectorId.startsWith('azure_blob_')) return 'Azure Blob Storage';
    if (connectorId.startsWith('azure_files_')) return 'Azure Files';
    if (connectorId.startsWith('azure_tables_queues_')) return 'Azure Tables & Queues';
    if (connectorId.startsWith('azure_data_storage_')) return 'Azure Data Storage';
    return 'Unknown';
  };

  const getDataSourceColor = (connectorId) => {
    if (!connectorId) return 'default';
    if (connectorId.startsWith('bq_')) return 'success';
    if (connectorId.startsWith('starburst_')) return 'info';
    if (connectorId.startsWith('s3_')) return 'warning';
    if (connectorId.startsWith('gcs_')) return 'primary';
    if (connectorId.startsWith('azure_blob_')) return 'secondary';
    if (connectorId.startsWith('azure_files_')) return 'secondary';
    if (connectorId.startsWith('azure_tables_queues_')) return 'secondary';
    if (connectorId.startsWith('azure_data_storage_')) return 'secondary';
    return 'default';
  };

  const handleViewAsset = async (assetId) => {
    try {
      setDetailsDialogOpen(true);
      setSelectedAsset(null); // Clear previous data
      const response = await fetch(`http://localhost:8099/api/assets/${encodeURIComponent(assetId)}`);
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      const data = await response.json();
      setSelectedAsset(data);
      // Initialize classification and sensitivity level from asset metadata
      const businessMetadata = data?.business_metadata || {};
      const initialClassification = businessMetadata.classification || 'internal';
      const initialSensitivityLevel = businessMetadata.sensitivity_level || 'medium';
      setClassification(initialClassification);
      setSensitivityLevel(initialSensitivityLevel);
      setOriginalClassification(initialClassification);
      setOriginalSensitivityLevel(initialSensitivityLevel);
      setActiveTab(0);
    } catch (error) {
      console.error('Error fetching asset details:', error);
      alert('Failed to load asset details. Please try again.');
      setDetailsDialogOpen(false);
    }
  };

  const handleCloseDialog = () => {
    setDetailsDialogOpen(false);
    setSelectedAsset(null);
    setActiveTab(0);
    setClassification('internal');
    setSensitivityLevel('medium');
    setOriginalClassification('internal');
    setOriginalSensitivityLevel('medium');
  };

  const handleSaveMetadata = async () => {
    if (!selectedAsset) return;
    
    setSavingMetadata(true);
    try {
      const response = await fetch(`http://localhost:8099/api/assets/${encodeURIComponent(selectedAsset.id)}`, {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          classification: classification,
          sensitivity_level: sensitivityLevel,
        }),
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }

      const data = await response.json();
      
      // Update the selected asset with new data
      setSelectedAsset(data.asset);
      
      // Update original values to match new saved values
      const businessMetadata = data.asset?.business_metadata || {};
      setOriginalClassification(businessMetadata.classification || 'internal');
      setOriginalSensitivityLevel(businessMetadata.sensitivity_level || 'medium');
      
      // Refresh assets list
      fetchAssets();
      
      alert('Metadata saved successfully!');
    } catch (error) {
      console.error('Error saving metadata:', error);
      alert('Failed to save metadata. Please try again.');
    } finally {
      setSavingMetadata(false);
    }
  };

  const handleTabChange = (event, newValue) => {
    setActiveTab(newValue);
  };

  // Pagination handlers
  const handlePageChange = (event, page) => {
    setCurrentPage(page - 1); // Convert to 0-based
  };

  const handlePageSizeChange = (event) => {
    setPageSize(event.target.value);
    setCurrentPage(0); // Reset to first page
  };

  // Search and filter handlers
  const handleSearchChange = (event) => {
    setSearchTerm(event.target.value);
    setCurrentPage(0); // Reset to first page
  };

  const handleTypeFilterChange = (event) => {
    setTypeFilter(event.target.value);
    setCurrentPage(0); // Reset to first page
  };

  const handleCatalogFilterChange = (event) => {
    setCatalogFilter(event.target.value);
    setCurrentPage(0); // Reset to first page
  };

  const formatBytes = (bytes) => {
    if (!bytes) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + ' ' + sizes[i];
  };

  const formatNumber = (num) => {
    if (!num) return '0';
    return num.toLocaleString();
  };

  const fetchPendingAssets = async () => {
    try {
      const response = await fetch('http://localhost:8099/api/s3/pending-assets');
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      const data = await response.json();
      setPendingAssets(data.pending_assets || []);
    } catch (error) {
      console.error('Error fetching pending assets:', error);
    }
  };

  const handleAcceptAsset = async (pendingId) => {
    try {
      const response = await fetch('http://localhost:8099/api/s3/accept-asset', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ pending_id: pendingId }),
      });
      
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      
      const data = await response.json();
      console.log('✅ Asset action completed:', data.message);
      console.log('📊 Assets count from backend:', data.assets_count);
      
      // Close dialog first
      setPendingDialogOpen(false);
      
      // Refresh pending assets first
      await fetchPendingAssets();
      
      // Reset to page 0 to see newly added asset at top
      setCurrentPage(0);
      
      // Small delay to ensure backend has processed the save
      await new Promise(resolve => setTimeout(resolve, 300));
      
      // Use the existing fetchAssets function which handles sorting correctly
      // Pass page 0 directly since state update is async
      console.log('🔄 Refreshing assets after accept...');
      await fetchAssets(0);
      
      // Force refresh totals to update cards
      console.log('🔄 Refreshing totals...');
      await fetchTotals();
      
      // Show success message
      console.log('✅ Asset added successfully! It should appear at the top of the list.');
    } catch (error) {
      console.error('Error accepting asset:', error);
      alert('Failed to process asset. Please try again.');
    }
  };

  const handleDismissAsset = async (pendingId) => {
    try {
      const response = await fetch('http://localhost:8099/api/s3/dismiss-asset', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ pending_id: pendingId }),
      });
      
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      
      // Refresh pending assets
      fetchPendingAssets();
    } catch (error) {
      console.error('Error dismissing asset:', error);
      alert('Failed to dismiss notification. Please try again.');
    }
  };

  const handleAcceptAll = async () => {
    for (const pending of pendingAssets) {
      await handleAcceptAsset(pending.id);
    }
    setPendingDialogOpen(false);
  };

  // Group pending assets by change type
  const createdAssets = pendingAssets.filter(a => a.change_type === 'created');
  const deletedAssets = pendingAssets.filter(a => a.change_type === 'deleted');

  return (
    <Box sx={{ p: 3 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
        <Typography variant="h4" component="h1" sx={{ fontWeight: 600, fontFamily: 'Comfortaa' }}>
          Discovered Assets
        </Typography>
        <Box sx={{ display: 'flex', gap: 2, alignItems: 'center' }}>
          {/* Notification Icon with Badge */}
          <Tooltip title={pendingAssets.length > 0 ? `${pendingAssets.length} new change${pendingAssets.length > 1 ? 's' : ''} detected` : 'No new notifications'}>
            <IconButton
              color={pendingAssets.length > 0 ? 'warning' : 'default'}
              onClick={async () => {
                if (pendingAssets.length > 0) {
                  setPendingDialogOpen(true);
                } else {
                  // Trigger instant discovery when bell is clicked
                  setDiscoveryLoading(true);
                  try {
                    console.log('🔔 Triggering instant discovery...');
                    const response = await fetch('http://localhost:8099/api/s3/trigger-discovery', {
                      method: 'POST',
                      headers: {
                        'Content-Type': 'application/json',
                      },
                    });
                    const data = await response.json();
                    console.log('Discovery response:', data);
                    
                    if (data.status === 'success') {
                      // Refresh pending assets after discovery
                      await fetchPendingAssets();
                      await fetchAssets(); // Refresh assets list too
                      
                      if (data.new_assets > 0) {
                        // Show success message
                        console.log(`✅ Discovered ${data.new_assets} new asset(s)!`);
                      } else {
                        setNoNotificationsSnackbar(true);
                      }
                    } else {
                      console.error('Discovery failed:', data.message);
                      alert(`Discovery failed: ${data.message || 'Unknown error'}`);
                    }
                  } catch (error) {
                    console.error('Error triggering discovery:', error);
                    alert(`Error: ${error.message}`);
                  } finally {
                    setDiscoveryLoading(false);
                  }
                }
              }}
              sx={{
                position: 'relative',
                ...(pendingAssets.length > 0 && {
                  animation: 'pulse 2s infinite',
                  '@keyframes pulse': {
                    '0%': {
                      boxShadow: '0 0 0 0 rgba(237, 108, 2, 0.7)',
                    },
                    '70%': {
                      boxShadow: '0 0 0 10px rgba(237, 108, 2, 0)',
                    },
                    '100%': {
                      boxShadow: '0 0 0 0 rgba(237, 108, 2, 0)',
                    },
                  },
                }),
              }}
            >
              <Badge
                badgeContent={pendingAssets.length}
                color="error"
                max={99}
                sx={{
                  '& .MuiBadge-badge': {
                    fontSize: '0.75rem',
                    height: '20px',
                    minWidth: '20px',
                    padding: '0 6px',
                  },
                }}
              >
                <Notifications />
              </Badge>
            </IconButton>
          </Tooltip>
          <Button
            variant="outlined"
            startIcon={<Refresh />}
            onClick={() => {
              fetchAssets();
              fetchPendingAssets(); // Also refresh pending assets
              if (!searchTerm && !typeFilter && !catalogFilter) {
                fetchTotals();
              }
            }}
            disabled={loading}
          >
            Refresh
          </Button>
          <Button
            variant="contained"
            startIcon={<Download />}
            color="primary"
          >
            Export
          </Button>
        </Box>
      </Box>

      <Grid container spacing={3} sx={{ mb: 3 }}>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                <DataObject sx={{ mr: 1, color: 'primary.main' }} />
                <Typography variant="h6">Total Assets</Typography>
              </Box>
              <Typography variant="h4" sx={{ fontWeight: 'bold' }}>
                {totalAssets}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                <DataObject sx={{ mr: 1, color: 'success.main' }} />
                <Typography variant="h6">BigQuery Assets</Typography>
              </Box>
              <Typography variant="h4" sx={{ fontWeight: 'bold' }}>
                {bigqueryTotal}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                <DataObject sx={{ mr: 1, color: 'info.main' }} />
                <Typography variant="h6">Starburst Assets</Typography>
              </Box>
              <Typography variant="h4" sx={{ fontWeight: 'bold' }}>
                {starburstTotal}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                <DataObject sx={{ mr: 1, color: 'warning.main' }} />
                <Typography variant="h6">Amazon S3 Assets</Typography>
              </Box>
              <Typography variant="h4" sx={{ fontWeight: 'bold' }}>
                {s3Total}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Grid container spacing={2} alignItems="center">
            <Grid item xs={12} md={4}>
                <TextField
                  fullWidth
                  placeholder="Search assets..."
                  value={searchTerm}
                  onChange={handleSearchChange}
                  InputProps={{
                    startAdornment: (
                      <InputAdornment position="start">
                        <Search />
                      </InputAdornment>
                    ),
                  }}
                />
            </Grid>
            <Grid item xs={12} md={3}>
              <FormControl fullWidth>
                <InputLabel>Type</InputLabel>
                <Select
                  value={typeFilter}
                  label="Type"
                  onChange={handleTypeFilterChange}
                >
                  <MenuItem value="">All Types</MenuItem>
                  {uniqueTypes.map(type => (
                    <MenuItem key={type} value={type}>{type}</MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={3}>
              <FormControl fullWidth>
                <InputLabel>Catalog</InputLabel>
                <Select
                  value={catalogFilter}
                  label="Catalog"
                  onChange={handleCatalogFilterChange}
                >
                  <MenuItem value="">All Catalogs</MenuItem>
                  {uniqueCatalogs.map(catalog => (
                    <MenuItem key={catalog} value={catalog}>{catalog}</MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={2}>
              <Button
                fullWidth
                variant="outlined"
                startIcon={<FilterList />}
                onClick={() => {
                  setSearchTerm('');
                  setTypeFilter('');
                  setCatalogFilter('');
                  setCurrentPage(0);
                }}
              >
                Clear
              </Button>
            </Grid>
          </Grid>
        </CardContent>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6" sx={{ mb: 2, fontWeight: 600, fontFamily: 'Comfortaa' }}>
            Asset Inventory ({totalAssets} assets)
          </Typography>
          <TableContainer>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell sx={{ width: '60px', fontWeight: 600 }}>#</TableCell>
                  <TableCell>Name</TableCell>
                  <TableCell>Type</TableCell>
                  <TableCell>Format</TableCell>
                  <TableCell>Catalog</TableCell>
                  <TableCell>Data Source</TableCell>
                  <TableCell>Discovered</TableCell>
                  <TableCell>Actions</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {assets.map((asset, index) => (
                  <TableRow key={asset.id}>
                    <TableCell sx={{ fontWeight: 500, color: 'text.secondary' }}>
                      {currentPage * pageSize + index + 1}
                    </TableCell>
                    <TableCell>
                      <Box sx={{ display: 'flex', alignItems: 'center' }}>
                        <DataObject sx={{ mr: 1, color: 'text.secondary' }} />
                        <Typography variant="body2" sx={{ fontWeight: 500, fontFamily: 'Roboto' }}>
                          {asset.name}
                        </Typography>
                      </Box>
                    </TableCell>
                    <TableCell>
                      <Chip 
                        label={asset.type} 
                        size="small" 
                        variant="outlined"
                      />
                    </TableCell>
                    <TableCell>
                      <Typography variant="body2" sx={{ fontFamily: 'Roboto', fontSize: '0.875rem' }}>
                        {asset.technical_metadata?.format || asset.technical_metadata?.content_type || 'Unknown'}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Typography variant="body2" sx={{ fontFamily: 'Roboto' }}>
                        {asset.catalog}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Chip 
                        label={getDataSource(asset.connector_id)} 
                        size="small" 
                        color={getDataSourceColor(asset.connector_id)}
                      />
                    </TableCell>
                    <TableCell>
                      <Typography variant="body2" color="text.secondary" sx={{ fontFamily: 'Roboto' }}>
                        {new Date(asset.discovered_at).toLocaleDateString()}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Button
                        size="small"
                        startIcon={<Visibility />}
                        variant="outlined"
                        onClick={() => handleViewAsset(asset.id)}
                      >
                        View
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
          
          {/* Pagination Controls */}
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mt: 3, px: 2 }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
              <Typography variant="body2" color="text.secondary">
                Showing {assets.length} of {totalAssets} assets
              </Typography>
              <FormControl size="small" sx={{ minWidth: 80 }}>
                <Select
                  value={pageSize}
                  onChange={handlePageSizeChange}
                  displayEmpty
                >
                  <MenuItem value={25}>25</MenuItem>
                  <MenuItem value={50}>50</MenuItem>
                  <MenuItem value={100}>100</MenuItem>
                </Select>
              </FormControl>
              <Typography variant="body2" color="text.secondary">
                per page
              </Typography>
            </Box>
            
            <Pagination
              count={totalPages}
              page={currentPage + 1}
              onChange={handlePageChange}
              color="primary"
              showFirstButton
              showLastButton
              disabled={loading}
            />
          </Box>
        </CardContent>
      </Card>

      {/* Asset Details Dialog */}
      <Dialog
        open={detailsDialogOpen}
        onClose={handleCloseDialog}
        maxWidth="lg"
        fullWidth
      >
        {!selectedAsset ? (
          <Box sx={{ p: 4, display: 'flex', flexDirection: 'column', justifyContent: 'center', alignItems: 'center', minHeight: 300, gap: 2 }}>
            <CircularProgress />
            <Typography>Loading asset details...</Typography>
          </Box>
        ) : (
          <>
            <DialogTitle>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <Box>
                  <Typography variant="h5" sx={{ fontWeight: 600 }}>
                    {selectedAsset.name}
                  </Typography>
                  <Box sx={{ display: 'flex', gap: 1, mt: 1 }}>
                    <Chip label={selectedAsset.type} size="small" color="primary" />
                    <Chip label={selectedAsset.catalog} size="small" variant="outlined" />
                  </Box>
                </Box>
                <Button onClick={handleCloseDialog} startIcon={<Close />}>
                  Close
                </Button>
              </Box>
            </DialogTitle>
            <DialogContent dividers>
              <Tabs value={activeTab} onChange={handleTabChange} sx={{ mb: 3 }}>
                <Tab label="Technical Metadata" />
                <Tab label="Operational Metadata" />
                <Tab label="Business Metadata" />
                <Tab label="Columns & PII" />
              </Tabs>

              {/* Technical Metadata Tab */}
              {activeTab === 0 && (
                <Box>
                  <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
                    Technical Metadata
                  </Typography>
                  {(() => {
                    // Defensive programming: ensure technical_metadata exists and has all required fields
                    const technicalMetadata = selectedAsset?.technical_metadata || {};
                    const safeAssetId = technicalMetadata.asset_id || selectedAsset?.id || 'N/A';
                    const safeAssetType = technicalMetadata.asset_type || selectedAsset?.type || 'Unknown';
                    const safeLocation = technicalMetadata.location || 'N/A';
                    const safeFormat = technicalMetadata.format || technicalMetadata.content_type || 'Unknown';
                    const safeSizeBytes = technicalMetadata.size_bytes || 0;
                    const safeNumRows = technicalMetadata.num_rows || 0;
                    const safeCreatedAt = technicalMetadata.created_at || selectedAsset?.discovered_at || new Date().toISOString();
                    const safeStorageClass = technicalMetadata.storage_class || 'N/A';
                    const safeRegion = technicalMetadata.region || 'N/A';
                    const safeBucketName = technicalMetadata.bucket_name || 'N/A';
                    const safeObjectKey = technicalMetadata.object_key || technicalMetadata.object_path || 'N/A';
                    const safeFileExtension = technicalMetadata.file_extension || 'N/A';
                    
                    return (
                      <Grid container spacing={2}>
                        <Grid item xs={6}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Asset ID
                              </Typography>
                              <Typography variant="body1" sx={{ wordBreak: 'break-all' }}>
                                {safeAssetId}
                              </Typography>
                            </CardContent>
                          </Card>
                        </Grid>
                        <Grid item xs={6}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Asset Type
                              </Typography>
                              <Typography variant="body1">
                                {safeAssetType}
                              </Typography>
                            </CardContent>
                          </Card>
                        </Grid>
                        <Grid item xs={6}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Location
                              </Typography>
                              <Typography variant="body1">
                                {safeLocation}
                              </Typography>
                            </CardContent>
                          </Card>
                        </Grid>
                        <Grid item xs={6}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Format
                              </Typography>
                              <Typography variant="body1">
                                {safeFormat}
                              </Typography>
                            </CardContent>
                          </Card>
                        </Grid>
                        <Grid item xs={6}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Size
                              </Typography>
                              <Typography variant="body1">
                                {formatBytes(safeSizeBytes)}
                              </Typography>
                            </CardContent>
                          </Card>
                        </Grid>
                        <Grid item xs={6}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Number of Rows
                              </Typography>
                              <Typography variant="body1">
                                {formatNumber(safeNumRows)}
                              </Typography>
                            </CardContent>
                          </Card>
                        </Grid>
                        {/* S3-specific metadata */}
                        {selectedAsset?.connector_id?.startsWith('s3_') && (
                          <>
                            <Grid item xs={6}>
                              <Card variant="outlined">
                                <CardContent>
                                  <Typography color="text.secondary" gutterBottom>
                                    Storage Class
                                  </Typography>
                                  <Typography variant="body1">
                                    {safeStorageClass}
                                  </Typography>
                                </CardContent>
                              </Card>
                            </Grid>
                            <Grid item xs={6}>
                              <Card variant="outlined">
                                <CardContent>
                                  <Typography color="text.secondary" gutterBottom>
                                    AWS Region
                                  </Typography>
                                  <Typography variant="body1">
                                    {safeRegion}
                                  </Typography>
                                </CardContent>
                              </Card>
                            </Grid>
                            <Grid item xs={6}>
                              <Card variant="outlined">
                                <CardContent>
                                  <Typography color="text.secondary" gutterBottom>
                                    Bucket Name
                                  </Typography>
                                  <Typography variant="body1" sx={{ wordBreak: 'break-all' }}>
                                    {safeBucketName}
                                  </Typography>
                                </CardContent>
                              </Card>
                            </Grid>
                            <Grid item xs={6}>
                              <Card variant="outlined">
                                <CardContent>
                                  <Typography color="text.secondary" gutterBottom>
                                    Object Key/Path
                                  </Typography>
                                  <Typography variant="body1" sx={{ wordBreak: 'break-all', fontSize: '0.875rem' }}>
                                    {safeObjectKey}
                                  </Typography>
                                </CardContent>
                              </Card>
                            </Grid>
                            {safeFileExtension !== 'N/A' && (
                              <Grid item xs={6}>
                                <Card variant="outlined">
                                  <CardContent>
                                    <Typography color="text.secondary" gutterBottom>
                                      File Extension
                                    </Typography>
                                    <Typography variant="body1">
                                      .{safeFileExtension}
                                    </Typography>
                                  </CardContent>
                                </Card>
                              </Grid>
                            )}
                            {technicalMetadata.content_type && (
                              <Grid item xs={6}>
                                <Card variant="outlined">
                                  <CardContent>
                                    <Typography color="text.secondary" gutterBottom>
                                      Content Type (MIME)
                                    </Typography>
                                    <Typography variant="body1" sx={{ fontSize: '0.875rem' }}>
                                      {technicalMetadata.content_type}
                                    </Typography>
                                  </CardContent>
                                </Card>
                              </Grid>
                            )}
                          </>
                        )}
                        <Grid item xs={12}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Created At
                              </Typography>
                              <Typography variant="body1">
                                {new Date(safeCreatedAt).toLocaleString()}
                              </Typography>
                            </CardContent>
                          </Card>
                        </Grid>
                      </Grid>
                    );
                  })()}
                </Box>
              )}

              {/* Operational Metadata Tab */}
              {activeTab === 1 && (
                <Box>
                  <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
                    Operational Metadata
                  </Typography>
                  {(() => {
                    // Defensive programming: ensure operational_metadata exists and has all required fields
                    const operationalMetadata = selectedAsset?.operational_metadata || {};
                    const safeStatus = operationalMetadata.status || 'Unknown';
                    const safeOwner = typeof operationalMetadata.owner === 'object' && operationalMetadata.owner?.roleName 
                      ? operationalMetadata.owner.roleName 
                      : operationalMetadata.owner || 'Unknown';
                    const safeLastModified = operationalMetadata.last_modified || selectedAsset?.discovered_at || new Date().toISOString();
                    const safeLastAccessed = operationalMetadata.last_accessed || new Date().toISOString();
                    const safeAccessCount = operationalMetadata.access_count || 'N/A';
                    const safeDataQualityScore = operationalMetadata.data_quality_score || 0;
                    
                    return (
                      <Grid container spacing={2}>
                        <Grid item xs={6}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Status
                              </Typography>
                              <Chip 
                                label={safeStatus} 
                                color="success" 
                                size="small"
                              />
                            </CardContent>
                          </Card>
                        </Grid>
                        <Grid item xs={6}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Owner
                              </Typography>
                              <Typography variant="body1">
                                {safeOwner}
                              </Typography>
                            </CardContent>
                          </Card>
                        </Grid>
                        <Grid item xs={6}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Last Modified
                              </Typography>
                              <Typography variant="body1">
                                {new Date(safeLastModified).toLocaleString()}
                              </Typography>
                            </CardContent>
                          </Card>
                        </Grid>
                        <Grid item xs={6}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Last Accessed
                              </Typography>
                              <Typography variant="body1">
                                {new Date(safeLastAccessed).toLocaleString()}
                              </Typography>
                            </CardContent>
                          </Card>
                        </Grid>
                        <Grid item xs={6}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Access Count
                              </Typography>
                              <Typography variant="body1">
                                {safeAccessCount}
                              </Typography>
                            </CardContent>
                          </Card>
                        </Grid>
                        <Grid item xs={6}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Data Quality Score
                              </Typography>
                              <Typography variant="body1">
                                {safeDataQualityScore}%
                              </Typography>
                            </CardContent>
                          </Card>
                        </Grid>
                      </Grid>
                    );
                  })()}
                </Box>
              )}

              {/* Business Metadata Tab */}
              {activeTab === 2 && (
                <Box>
                  <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
                    Business Metadata
                  </Typography>
                  {(() => {
                    // Defensive programming: ensure business_metadata exists and has all required fields
                    const businessMetadata = selectedAsset?.business_metadata || {};
                    const safeDescription = businessMetadata.description || selectedAsset?.description || 'No description available';
                    const safeBusinessOwner = businessMetadata.business_owner || 'Unknown';
                    const safeDepartment = businessMetadata.department || 'N/A';
                    const safeClassification = businessMetadata.classification || 'internal';
                    const safeSensitivityLevel = businessMetadata.sensitivity_level || 'medium';
                    const safeTags = businessMetadata.tags || [];
                    
                    return (
                      <Grid container spacing={2}>
                        <Grid item xs={12}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Description
                              </Typography>
                              <Typography variant="body1">
                                {safeDescription}
                              </Typography>
                            </CardContent>
                          </Card>
                        </Grid>
                        <Grid item xs={6}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Business Owner
                              </Typography>
                              <Typography variant="body1">
                                {safeBusinessOwner}
                              </Typography>
                            </CardContent>
                          </Card>
                        </Grid>
                        <Grid item xs={6}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Department
                              </Typography>
                              <Typography variant="body1">
                                {safeDepartment}
                              </Typography>
                            </CardContent>
                          </Card>
                        </Grid>
                        <Grid item xs={6}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Classification
                              </Typography>
                              <FormControl fullWidth size="small" sx={{ mt: 1 }}>
                                <Select
                                  value={classification}
                                  onChange={(e) => setClassification(e.target.value)}
                                  displayEmpty
                                >
                                  <MenuItem value="public">Public</MenuItem>
                                  <MenuItem value="internal">Internal</MenuItem>
                                  <MenuItem value="confidential">Confidential</MenuItem>
                                  <MenuItem value="restricted">Restricted</MenuItem>
                                  <MenuItem value="top_secret">Top Secret</MenuItem>
                                </Select>
                              </FormControl>
                            </CardContent>
                          </Card>
                        </Grid>
                        <Grid item xs={6}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Sensitivity Level
                              </Typography>
                              <FormControl fullWidth size="small" sx={{ mt: 1 }}>
                                <Select
                                  value={sensitivityLevel}
                                  onChange={(e) => setSensitivityLevel(e.target.value)}
                                  displayEmpty
                                >
                                  <MenuItem value="low">Low</MenuItem>
                                  <MenuItem value="medium">Medium</MenuItem>
                                  <MenuItem value="high">High</MenuItem>
                                  <MenuItem value="critical">Critical</MenuItem>
                                </Select>
                              </FormControl>
                            </CardContent>
                          </Card>
                        </Grid>
                        <Grid item xs={12}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom sx={{ fontWeight: 600 }}>
                                Table Tags
                              </Typography>
                              <Box sx={{ display: 'flex', gap: 1, mt: 1, flexWrap: 'wrap' }}>
                                {safeTags && safeTags.length > 0 ? (
                                  safeTags.map((tag, index) => (
                                    <Chip 
                                      key={index} 
                                      label={tag} 
                                      size="small" 
                                      variant="outlined"
                                      sx={{ 
                                        backgroundColor: '#e3f2fd', 
                                        color: '#1565c0', 
                                        border: '1px solid #90caf9',
                                        fontWeight: 600
                                      }}
                                    />
                                  ))
                                ) : (
                                  <Typography variant="body2" color="text.secondary" sx={{ fontStyle: 'italic' }}>
                                    No table tags
                                  </Typography>
                                )}
                              </Box>
                            </CardContent>
                          </Card>
                        </Grid>
                        <Grid item xs={12}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom sx={{ fontWeight: 600 }}>
                                Column Tags
                              </Typography>
                              <Box sx={{ mt: 1 }}>
                                {selectedAsset?.columns && selectedAsset.columns.length > 0 ? (
                                  selectedAsset.columns.map((column, colIndex) => {
                                    // ONLY show real tags from column.tags field, NOT from description
                                    const columnTags = column.tags || [];
                                    
                                    if (columnTags.length > 0) {
                                      return (
                                        <Box key={colIndex} sx={{ mb: 2, pb: 2, borderBottom: colIndex < selectedAsset.columns.length - 1 ? '1px solid #e0e0e0' : 'none' }}>
                                          <Typography variant="body2" sx={{ fontWeight: 600, mb: 1, color: '#1976d2' }}>
                                            {column.name}
                                          </Typography>
                                          <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap' }}>
                                            {columnTags.map((tag, tagIndex) => (
                                              <Chip 
                                                key={tagIndex} 
                                                label={tag} 
                                                size="small" 
                                                variant="outlined"
                                                sx={{ 
                                                  backgroundColor: '#f3e5f5', 
                                                  color: '#7b1fa2', 
                                                  border: '1px solid #ce93d8',
                                                  fontWeight: 600
                                                }}
                                              />
                                            ))}
                                          </Box>
                                        </Box>
                                      );
                                    }
                                    return null;
                                  }).filter(Boolean)
                                ) : null}
                                {(!selectedAsset?.columns || selectedAsset.columns.length === 0 || 
                                  !selectedAsset.columns.some(col => col.tags && col.tags.length > 0)) && (
                                  <Typography variant="body2" color="text.secondary" sx={{ fontStyle: 'italic' }}>
                                    No column tags
                                  </Typography>
                                )}
                              </Box>
                            </CardContent>
                          </Card>
                        </Grid>
                      </Grid>
                    );
                  })()}
                </Box>
              )}

              {/* Columns & PII Tab */}
              {activeTab === 3 && (
                <Box>
                  <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
                    Columns & PII Detection
                  </Typography>
                  {(() => {
                    // Defensive programming: ensure columns exist and handle missing data
                    const columns = selectedAsset?.columns || [];
                    const piiColumns = columns.filter(col => col.pii_detected);
                    
                    if (columns.length > 0) {
                      return (
                        <>
                          {piiColumns.length > 0 && (
                            <Alert severity="warning" sx={{ mb: 2 }}>
                              {piiColumns.length} column(s) contain PII data
                            </Alert>
                          )}
                          <TableContainer component={Paper} variant="outlined">
                            <Table>
                              <TableHead>
                                <TableRow>
                                  <TableCell>Column Name</TableCell>
                                  <TableCell>Data Type</TableCell>
                                  <TableCell>Nullable</TableCell>
                                  <TableCell>Description</TableCell>
                                  <TableCell>PII Status</TableCell>
                                </TableRow>
                              </TableHead>
                              <TableBody>
                                {columns.map((column, index) => (
                                  <TableRow key={index}>
                                    <TableCell>
                                      <Typography variant="body2" sx={{ fontWeight: 500 }}>
                                        {column.name || 'Unknown'}
                                      </Typography>
                                    </TableCell>
                                    <TableCell>
                                      <Chip label={column.type || 'Unknown'} size="small" variant="outlined" />
                                    </TableCell>
                                    <TableCell>
                                      {column.nullable ? 'Yes' : 'No'}
                                    </TableCell>
                                    <TableCell>
                                      <Typography variant="body2" color="text.secondary">
                                        {column.description || 'No description'}
                                      </Typography>
                                    </TableCell>
                                    <TableCell>
                                      {column.pii_detected ? (
                                        <Chip 
                                          icon={<Warning />}
                                          label={`PII: ${column.pii_type || 'Unknown'}`} 
                                          color="error" 
                                          size="small"
                                        />
                                      ) : (
                                        <Chip 
                                          icon={<CheckCircle />}
                                          label="No PII" 
                                          color="success" 
                                          size="small"
                                        />
                                      )}
                                    </TableCell>
                                  </TableRow>
                                ))}
                              </TableBody>
                            </Table>
                          </TableContainer>
                        </>
                      );
                    } else {
                      return (
                        <Alert severity="info">
                          No column information available for this asset type.
                        </Alert>
                      );
                    }
                  })()}
                </Box>
              )}
            </DialogContent>
            <DialogActions>
              {/* Show Cancel and Save Changes only when Business Metadata tab is active OR values have changed */}
              {(activeTab === 2 || classification !== originalClassification || sensitivityLevel !== originalSensitivityLevel) && (
                <>
                  <Button 
                    onClick={handleCloseDialog} 
                    variant="outlined"
                    disabled={savingMetadata}
                  >
                    Cancel
                  </Button>
                  <Button
                    variant="contained"
                    color="primary"
                    onClick={handleSaveMetadata}
                    disabled={savingMetadata}
                    startIcon={savingMetadata ? <CircularProgress size={20} /> : null}
                  >
                    {savingMetadata ? 'Saving...' : 'Save Changes'}
                  </Button>
                </>
              )}
              <Button onClick={handleCloseDialog} variant="outlined">
                Close
              </Button>
            </DialogActions>
          </>
        )}
      </Dialog>

      {/* Pending Assets Dialog */}
      <Dialog
        open={pendingDialogOpen}
        onClose={() => setPendingDialogOpen(false)}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <Typography variant="h5" sx={{ fontWeight: 600 }}>
              New Changes Detected
            </Typography>
            <Button onClick={() => setPendingDialogOpen(false)} startIcon={<Close />}>
              Close
            </Button>
          </Box>
        </DialogTitle>
        <DialogContent dividers>
          <Box sx={{ mb: 2 }}>
            <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
              Review and accept or dismiss the detected changes:
            </Typography>
            
            {createdAssets.length > 0 && (
              <Box sx={{ mb: 3 }}>
                <Typography variant="h6" sx={{ mb: 1, fontWeight: 600, color: 'success.main' }}>
                  Created ({createdAssets.length})
                </Typography>
                <TableContainer component={Paper} variant="outlined">
                  <Table size="small" sx={{ tableLayout: 'fixed', width: '100%' }}>
                    <TableHead>
                      <TableRow>
                        <TableCell sx={{ width: '35%' }}>Name</TableCell>
                        <TableCell sx={{ width: '15%' }}>Type</TableCell>
                        <TableCell sx={{ width: '25%' }}>Bucket Name</TableCell>
                        <TableCell sx={{ width: '25%' }}>Actions</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {createdAssets.map((pending) => (
                        <TableRow key={pending.id}>
                          <TableCell>{pending.name}</TableCell>
                          <TableCell>
                            <Chip label={pending.type} size="small" variant="outlined" />
                          </TableCell>
                          <TableCell>{pending.catalog}</TableCell>
                          <TableCell>
                            <Button
                              size="small"
                              variant="contained"
                              color="success"
                              onClick={() => handleAcceptAsset(pending.id)}
                              sx={{ mr: 1 }}
                            >
                              Add
                            </Button>
                            <Button
                              size="small"
                              variant="outlined"
                              onClick={() => handleDismissAsset(pending.id)}
                            >
                              Dismiss
                            </Button>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
              </Box>
            )}

            {deletedAssets.length > 0 && (
              <Box>
                <Typography variant="h6" sx={{ mb: 1, fontWeight: 600, color: 'error.main' }}>
                  Deleted ({deletedAssets.length})
                </Typography>
                <TableContainer component={Paper} variant="outlined">
                  <Table size="small" sx={{ tableLayout: 'fixed', width: '100%' }}>
                    <TableHead>
                      <TableRow>
                        <TableCell sx={{ width: '35%' }}>Name</TableCell>
                        <TableCell sx={{ width: '15%' }}>Type</TableCell>
                        <TableCell sx={{ width: '25%' }}>Bucket Name</TableCell>
                        <TableCell sx={{ width: '25%' }}>Actions</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {deletedAssets.map((pending) => (
                        <TableRow key={pending.id}>
                          <TableCell>{pending.name}</TableCell>
                          <TableCell>
                            <Chip label={pending.type} size="small" variant="outlined" />
                          </TableCell>
                          <TableCell>{pending.catalog}</TableCell>
                          <TableCell>
                            <Button
                              size="small"
                              variant="contained"
                              color="error"
                              onClick={() => handleAcceptAsset(pending.id)}
                              sx={{ mr: 1 }}
                            >
                              Remove
                            </Button>
                            <Button
                              size="small"
                              variant="outlined"
                              onClick={() => handleDismissAsset(pending.id)}
                            >
                              Dismiss
                            </Button>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
              </Box>
            )}
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setPendingDialogOpen(false)}>
            Close
          </Button>
          {createdAssets.length > 0 && (
            <Button
              variant="contained"
              color="success"
              onClick={handleAcceptAll}
            >
              Add All Created ({createdAssets.length})
            </Button>
          )}
        </DialogActions>
      </Dialog>

      {/* Snackbar for no notifications */}
      <Snackbar
        open={noNotificationsSnackbar}
        autoHideDuration={3000}
        onClose={() => setNoNotificationsSnackbar(false)}
        anchorOrigin={{ vertical: 'top', horizontal: 'center' }}
      >
        <Alert onClose={() => setNoNotificationsSnackbar(false)} severity="info" sx={{ width: '100%' }}>
          No new notifications available
        </Alert>
      </Snackbar>

      {/* AI Chatbot Assistant */}
      <AssetsChatbot assets={allAssets.length > 0 ? allAssets : assets} />
    </Box>
  );
};

export default AssetsPage;
