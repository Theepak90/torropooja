import React, { useState, useEffect, useRef } from 'react';
import { io } from 'socket.io-client';
import {
  Box,
  Typography,
  Card,
  CardContent,
  Grid,
  Button,
  Avatar,
  Chip,
  Divider,
  Dialog,
  DialogContent,
  IconButton,
  Stepper,
  Step,
  StepLabel,
  TextField,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  RadioGroup,
  FormControlLabel,
  Radio,
  Alert,
  CircularProgress,
} from '@mui/material';
import {
  Add,
  Refresh,
  CloudSync,
  CheckCircle,
  Error,
  CloudQueue,
  Storage,
  Cloud,
  Close,
  ArrowBack,
  ArrowForward,
  Delete,
  Visibility,
  Replay,
} from '@mui/icons-material';

const ConnectorsPage = () => {
  const [myConnections, setMyConnections] = useState([]);
  const [loading, setLoading] = useState(false);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [connectionToDelete, setConnectionToDelete] = useState(null);
  
  // Connection wizard state
  const [wizardOpen, setWizardOpen] = useState(false);
  const [selectedConnector, setSelectedConnector] = useState(null);
  const [activeStep, setActiveStep] = useState(0);
  const [connectionType, setConnectionType] = useState('');
  const [config, setConfig] = useState({});
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState(null);
  const [discoveryProgress, setDiscoveryProgress] = useState([]);
  const [logoErrors, setLogoErrors] = useState(new Set());
  

  // Use ref to keep socket instance stable
  const socketRef = useRef(null);

  useEffect(() => {
    fetchMyConnections();

    // Only initialize Socket.IO if not already connected
    if (!socketRef.current || socketRef.current.disconnected) {
      console.log('🔧 Initializing Socket.IO connection to /connectors namespace...');
      socketRef.current = io('http://localhost:8099/connectors', {
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
    } else {
      console.log('✅ Socket already connected, reusing existing connection');
    }

    const socket = socketRef.current;

    // Socket.IO connection event listeners (simplified - no reconnection)
    socket.on('connect', () => {
      console.log('✅ Socket.IO Connected to /connectors namespace! ID:', socket.id);
    });

    socket.on('disconnect', (reason) => {
      console.log('❌ Socket.IO Disconnected from /connectors namespace. Reason:', reason);
    });

    socket.on('connect_error', (error) => {
      console.error('❌ Socket.IO Connection Error:', error.message);
    });

    // Socket.IO event listeners for test connection
    socket.on('progress', (data) => {
      console.log('🔵 SocketIO Progress:', data);
      setDiscoveryProgress(prev => [...prev, data.message]);
    });

    socket.on('complete', (data) => {
      console.log('✅ SocketIO Complete:', data);
      setTestResult({
        success: true,
        message: data.message,
        discoveredAssets: data.discovered_assets,
        connectorId: data.connector_id,
        containers: data.containers || [],
        accountName: data.account_name,
        totalContainers: data.total_containers || 0
      });
      setTesting(false);
      fetchMyConnections();
    });

    socket.on('error', (data) => {
      console.error('❌ SocketIO Error:', data);
      setTestResult({
        success: false,
        message: data.message
      });
      setTesting(false);
    });


    // Clean up on component unmount
    return () => {
      if (socket) {
        console.log('🔴 Cleaning up socket connection...');
        // Remove all listeners to prevent memory leaks
        socket.off('connect');
        socket.off('disconnect');
        socket.off('connect_error');
        socket.off('progress');
        socket.off('complete');
        socket.off('error');
        // Disconnect the socket
        socket.disconnect();
        // Clear the ref
        socketRef.current = null;
      }
    };
  }, []); // Empty dependency array means this runs once on mount

  const fetchMyConnections = async () => {
    try {
      setLoading(true);
      const response = await fetch('http://localhost:8099/api/connectors');
      const data = await response.json();
      setMyConnections(data);
    } catch (error) {
      console.error('Error fetching connections:', error);
    } finally {
      setLoading(false);
    }
  };


  const handleDeleteClick = (connection) => {
    setConnectionToDelete(connection);
    setDeleteDialogOpen(true);
  };

  const handleDeleteConfirm = async () => {
    if (!connectionToDelete) return;
    
    try {
      // Call delete API endpoint
      const response = await fetch(`http://localhost:8099/api/connectors/${connectionToDelete.id}`, {
        method: 'DELETE',
      });
      
      if (response.ok) {
        const data = await response.json();
        console.log('✅ Delete successful:', data);
        
        // Remove from local state
        setMyConnections(prev => prev.filter(conn => conn.id !== connectionToDelete.id));
        setDeleteDialogOpen(false);
        setConnectionToDelete(null);
        
        // Refresh the connections list to ensure UI is in sync
        fetchMyConnections();
      } else {
        const errorData = await response.json().catch(() => ({ message: 'Failed to delete connection' }));
        console.error('❌ Failed to delete connection:', errorData);
        alert(`Failed to delete connection: ${errorData.message || 'Unknown error'}`);
      }
    } catch (error) {
      console.error('❌ Error deleting connection:', error);
      alert(`Error deleting connection: ${error.message}`);
    }
  };

  const handleDeleteCancel = () => {
    setDeleteDialogOpen(false);
    setConnectionToDelete(null);
  };

  const availableConnectors = [
    {
      id: 'bigquery',
      name: 'BigQuery',
      description: 'Google Cloud data warehouse for analytics',
      logo: 'https://www.vectorlogo.zone/logos/google_bigquery/google_bigquery-icon.svg',
      fallbackIcon: <CloudQueue />,
      color: '#4285F4',
      connectionTypes: ['Service Account'],
    },
    {
      id: 'starburst',
      name: 'Starburst Galaxy',
      description: 'Distributed SQL query engine for data lakes',
      logo: 'https://www.vectorlogo.zone/logos/starburst/starburst-icon.svg',
      fallbackIcon: <Storage />,
      color: '#00D4AA',
      connectionTypes: ['API Token'],
    },
    {
      id: 's3',
      name: 'Amazon S3',
      description: 'Amazon Simple Storage Service for object storage',
      logo: 'https://upload.wikimedia.org/wikipedia/commons/9/93/Amazon_Web_Services_Logo.svg',
      fallbackIcon: <Cloud />,
      color: '#FF9900',
      connectionTypes: ['Access Key'],
    },
    {
      id: 'gcs',
      name: 'Google Cloud Storage',
      description: 'Google Cloud object storage service',
      logo: 'https://www.vectorlogo.zone/logos/google_cloud/google_cloud-icon.svg',
      fallbackIcon: <Cloud />,
      color: '#4285F4',
      connectionTypes: ['Service Account'],
    },
    {
      id: 'azure-data-storage',
      name: 'Azure Data Storage',
      description: 'Microsoft Azure storage services (Blob Storage, ADLS Gen2, Files, Tables & Queues)',
      logo: 'https://www.vectorlogo.zone/logos/microsoft_azure/microsoft_azure-icon.svg',
      fallbackIcon: <Cloud />,
      color: '#0078D4',
      connectionTypes: ['Account Key', 'Connection String'],
    },
  ];

  const wizardSteps = [
    'Connection Type',
    'Configuration',
    'Test Connection',
    'Summary'
  ];

  const handleConnectClick = (connector) => {
    setSelectedConnector(connector);
    setActiveStep(0);
    setConnectionType('');
    setConfig({});
    setTestResult(null);
    setWizardOpen(true);
  };

  const handleNext = () => {
    setActiveStep((prevActiveStep) => prevActiveStep + 1);
  };

  const handleBack = () => {
    setActiveStep((prevActiveStep) => prevActiveStep - 1);
  };

  const handleWizardClose = () => {
    setWizardOpen(false);
    setSelectedConnector(null);
    setActiveStep(0);
    setConnectionType('');
    setConfig({});
    setTestResult(null);
  };

  const handleTestConnection = async () => {
    console.log('🔵 TEST CONNECTION clicked!');
    console.log('Connection Type:', connectionType);
    console.log('Selected Connector:', selectedConnector);
    console.log('Selected Connector ID:', selectedConnector?.id);
    console.log('Config:', config);
    
    // Validate that we have the connector ID
    if (!selectedConnector || !selectedConnector.id) {
      console.error('❌ No connector selected or connector missing ID!');
      setTestResult({ success: false, message: 'No connector selected. Please select a connector first.' });
      setTesting(false);
      return;
    }
    
    setTesting(true);
    setTestResult(null);
    setDiscoveryProgress([]);
    
    try {
      // All connectors use Socket.IO for streaming progress
      // Emit the test_connection event over Socket.IO
      const socket = socketRef.current;
      console.log('🟢 Preparing to emit socket event: test_connection');
      console.log('Socket connected?', socket?.connected);
      
      // Ensure we're sending the ID as a string
      const connectorId = String(selectedConnector.id);
      console.log('Event data:', { connectionType, selectedConnectorId: connectorId, config });
      
      if (!socket) {
        console.error('❌ Socket not initialized!');
        setTestResult({ success: false, message: 'Socket connection not available' });
        setTesting(false);
        return;
      }
      
      // Ensure socket is connected before emitting
      if (!socket.connected) {
        console.log('⚠️ Socket not connected, connecting now...');
        socket.connect();
        await new Promise(resolve => {
          socket.once('connect', resolve);
          setTimeout(resolve, 5000); // Timeout after 5 seconds
        });
      }
      
      console.log('✅ Socket connected! Emitting event to namespace...');
      // Send the config data directly, not wrapped in another object
      // Make sure selectedConnectorId is a string
      socket.emit('test_connection', { 
        connectionType: connectionType, 
        selectedConnectorId: connectorId, 
        config: config 
      });
      
      console.log('✅ Socket event emitted with connectorId:', connectorId);
      console.log('⏳ Waiting for streaming response...');
      // Don't set testing to false here - wait for socket response

    } catch (error) {
      console.error('Error testing connection:', error);
      setTestResult({ 
        success: false, 
        message: `Connection failed: ${error.message}` 
      });
      setTesting(false);
    }
  };

  const renderStepContent = (step) => {
    switch (step) {
      case 0:
        return (
          <Box>
            <Typography variant="h6" sx={{ mb: 2 }}>
              Select Connection Type for {selectedConnector?.name}
            </Typography>
            <FormControl component="fieldset">
              <RadioGroup
                value={connectionType}
                onChange={(e) => setConnectionType(e.target.value)}
              >
                {selectedConnector?.connectionTypes.map((type) => (
                  <FormControlLabel
                    key={type}
                    value={type}
                    control={<Radio />}
                    label={type}
                  />
                ))}
              </RadioGroup>
            </FormControl>
          </Box>
        );
      
      case 1:
        return (
          <Box>
            <Typography variant="h6" sx={{ mb: 2 }}>
              Configuration
            </Typography>
            <Grid container spacing={2}>
              <Grid item xs={12}>
                <TextField
                  fullWidth
                  label="Connection Name"
                  value={config.name || ''}
                  onChange={(e) => setConfig({...config, name: e.target.value})}
                />
              </Grid>
              {connectionType === 'Service Account' && selectedConnector?.id === 'bigquery' && (
                <>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      required
                      label="Project ID"
                      value={config.projectId || ''}
                      onChange={(e) => setConfig({...config, projectId: e.target.value})}
                      placeholder="your-gcp-project-id"
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      required
                      multiline
                      rows={10}
                      label="Service Account JSON"
                      value={config.serviceAccount || ''}
                      onChange={(e) => setConfig({...config, serviceAccount: e.target.value})}
                      placeholder={`Enter your service account JSON and make sure you have all the required permissions:\n\nRequired Permissions:\n• bigquery.datasets.get - View dataset metadata\n• bigquery.tables.list - List tables in datasets\n• bigquery.tables.get - View table metadata and schema\n• bigquery.tables.getData - Read table data for profiling\n• bigquery.tables.create - Create views\n• bigquery.tables.update - Modify tables and views\n• bigquery.tables.updateTag - Add/remove tags\n• datacatalog.taxonomies.get - Access data catalog\n• datacatalog.entries.list - List catalog entries\n• datacatalog.entries.updateTag - Manage data catalog tags\n\nPaste your service account JSON here...`}
                      helperText="Ensure the service account has all permissions listed above for data discovery, lineage, tagging, and view management"
                    />
                  </Grid>
                </>
              )}
              {connectionType === 'Service Account' && selectedConnector?.id === 'gcs' && (
                <>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      required
                      label="Project ID"
                      value={config.projectId || ''}
                      onChange={(e) => setConfig({...config, projectId: e.target.value})}
                      placeholder="your-gcp-project-id"
                      helperText="Your Google Cloud Project ID"
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      required
                      multiline
                      rows={10}
                      label="Service Account JSON"
                      value={config.serviceAccount || ''}
                      onChange={(e) => setConfig({...config, serviceAccount: e.target.value})}
                      placeholder="Paste your service account JSON here..."
                      helperText="Service account with Storage Object Viewer or Storage Admin permissions"
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      label="Bucket Name (Optional)"
                      value={config.bucketName || ''}
                      onChange={(e) => setConfig({...config, bucketName: e.target.value})}
                      placeholder="my-bucket-name"
                      helperText="Specific bucket to connect to. Leave empty to discover all accessible buckets."
                    />
                  </Grid>
                </>
              )}
              {connectionType === 'API Token' && selectedConnector?.id === 'starburst' && (
                <>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      required
                      label="Account Domain"
                      value={config.accountDomain || ''}
                      onChange={(e) => setConfig({...config, accountDomain: e.target.value})}
                      placeholder="e.g., mycompany.galaxy.starburst.io"
                      helperText="Your Starburst Galaxy account domain"
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      required
                      label="Client ID"
                      value={config.clientId || ''}
                      onChange={(e) => setConfig({...config, clientId: e.target.value})}
                      helperText="OAuth Client ID from Starburst Galaxy"
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      required
                      label="Client Secret"
                      type="password"
                      value={config.clientSecret || ''}
                      onChange={(e) => setConfig({...config, clientSecret: e.target.value})}
                      helperText="OAuth Client Secret from Starburst Galaxy"
                    />
                  </Grid>
                </>
              )}
              {connectionType === 'Access Key' && selectedConnector?.id === 's3' && (
                <>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      required
                      label="AWS Access Key ID"
                      value={config.accessKeyId || ''}
                      onChange={(e) => setConfig({...config, accessKeyId: e.target.value})}
                      placeholder="AKIAIOSFODNN7EXAMPLE"
                      helperText="Your AWS Access Key ID"
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      required
                      label="AWS Secret Access Key"
                      type="password"
                      value={config.secretAccessKey || ''}
                      onChange={(e) => setConfig({...config, secretAccessKey: e.target.value})}
                      helperText="Your AWS Secret Access Key"
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      label="AWS Region (Optional)"
                      value={config.region || ''}
                      onChange={(e) => setConfig({...config, region: e.target.value})}
                      placeholder="e.g., us-east-1 (leave empty to discover buckets from all regions)"
                      helperText="AWS region for S3 client. Leave empty to discover buckets from all regions automatically."
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      label="Bucket Name (Optional)"
                      value={config.bucketName || ''}
                      onChange={(e) => setConfig({...config, bucketName: e.target.value})}
                      placeholder="my-bucket-name"
                      helperText="Specific bucket to connect to. Leave empty to discover all accessible buckets."
                    />
                  </Grid>
                </>
              )}
              {(connectionType === 'Account Key' || connectionType === 'Connection String') && selectedConnector?.id === 'azure-data-storage' && (
                <>
                  {connectionType === 'Account Key' && (
                    <>
                      <Grid item xs={12}>
                        <TextField
                          fullWidth
                          required
                          label="Account Name"
                          value={config.accountName || ''}
                          onChange={(e) => setConfig({...config, accountName: e.target.value})}
                          placeholder="mystorageaccount"
                          helperText="Your Azure Storage Account name"
                        />
                      </Grid>
                      <Grid item xs={12}>
                        <TextField
                          fullWidth
                          required
                          label="Account Key"
                          type="password"
                          value={config.accountKey || ''}
                          onChange={(e) => setConfig({...config, accountKey: e.target.value})}
                          helperText="Your Azure Storage Account key"
                        />
                      </Grid>
                    </>
                  )}
                  {connectionType === 'Connection String' && (
                    <Grid item xs={12}>
                      <TextField
                        fullWidth
                        required
                        multiline
                        rows={3}
                        label="Connection String"
                        value={config.connectionString || ''}
                        onChange={(e) => setConfig({...config, connectionString: e.target.value})}
                        placeholder="DefaultEndpointsProtocol=https;AccountName=..."
                        helperText="Your Azure Storage connection string"
                      />
                    </Grid>
                  )}
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      label="Container Name (Optional)"
                      value={config.containerName || ''}
                      onChange={(e) => setConfig({...config, containerName: e.target.value})}
                      placeholder="my-container"
                      helperText="Specific blob container to discover. Leave empty to discover all containers."
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      label="File Share Name (Optional)"
                      value={config.shareName || ''}
                      onChange={(e) => setConfig({...config, shareName: e.target.value})}
                      placeholder="my-share"
                      helperText="Specific file share to discover. Leave empty to discover all file shares."
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      type="number"
                      label="Rediscovery Interval (Minutes)"
                      value={config.rediscovery_interval_minutes || 5}
                      onChange={(e) => setConfig({...config, rediscovery_interval_minutes: parseInt(e.target.value) || 5})}
                      inputProps={{ min: 1, max: 1440 }}
                      helperText="How often to automatically rediscover assets (default: 5 minutes). Minimum: 1 minute, Maximum: 1440 minutes (24 hours)."
                    />
                  </Grid>
                </>
              )}
            </Grid>
          </Box>
        );
      
      case 2:
        return (
          <Box>
            <Typography variant="h6" sx={{ mb: 2 }}>
              Test Connection
            </Typography>
            <Box sx={{ textAlign: 'center', py: 4 }}>
              {testing ? (
                <Box>
                  <CircularProgress sx={{ mb: 2 }} />
                  <Typography sx={{ mb: 3, fontWeight: 600 }}>Testing connection...</Typography>
                  
                  {/* Real-time Discovery Progress */}
                  {discoveryProgress.length > 0 && (
                    <Box sx={{ 
                      maxHeight: '300px', 
                      overflowY: 'auto', 
                      bgcolor: '#f5f5f5', 
                      p: 2, 
                      borderRadius: 1,
                      textAlign: 'left',
                      fontFamily: 'monospace',
                      fontSize: '0.875rem'
                    }}>
                      {discoveryProgress.map((message, index) => (
                        <Box key={index} sx={{ 
                          py: 0.5,
                          color: message.includes('✓') ? 'success.main' : 
                                 message.includes('✗') ? 'error.main' : 
                                 message.includes('Discovering') ? 'primary.main' : 'text.primary',
                          display: 'flex',
                          alignItems: 'center',
                          gap: 1
                        }}>
                          {message.includes('✓') && <CheckCircle sx={{ fontSize: 16 }} />}
                          <span>{message}</span>
                        </Box>
                      ))}
                    </Box>
                  )}
                </Box>
              ) : testResult ? (
                <Box>
                  {/* Only show error alerts, hide success alert */}
                  {!testResult.success && (
                    <Alert severity="error" sx={{ mb: 2 }}>
                      {testResult.message}
                    </Alert>
                  )}
                  
                  {/* Show dark green success box */}
                  {testResult.success && testResult.discoveredAssets > 0 && (
                    <Card variant="outlined" sx={{ p: 2, mb: 3, bgcolor: 'success.light', color: 'success.dark' }}>
                      <Typography variant="h6" sx={{ fontWeight: 600, mb: 1 }}>
                        🎉 Discovery Complete!
                      </Typography>
                      <Typography variant="body1">
                        <strong>{testResult.discoveredAssets}</strong> assets discovered across <strong>{testResult.totalContainers || 0}</strong> container(s)
                      </Typography>
                      {testResult.accountName && (
                        <Typography variant="body2" sx={{ mt: 0.5, opacity: 0.9 }}>
                          Account: {testResult.accountName}
                        </Typography>
                      )}
                      <Typography variant="body2" sx={{ mt: 1, opacity: 0.9 }}>
                        View them in the "Discovered Assets" section
                      </Typography>
                    </Card>
                  )}
                  
                  
                  {/* Show discovery log with all progress */}
                  {discoveryProgress.length > 0 && (
                    <Box>
                      <Typography variant="subtitle2" sx={{ mb: 1, textAlign: 'left', fontWeight: 600 }}>
                        Discovery Progress:
                      </Typography>
                      <Box sx={{ 
                        maxHeight: '300px', 
                        overflowY: 'auto', 
                        bgcolor: '#f5f5f5', 
                        p: 2, 
                        borderRadius: 1,
                        textAlign: 'left',
                        fontFamily: 'monospace',
                        fontSize: '0.875rem',
                        border: '1px solid #e0e0e0'
                      }}>
                        {discoveryProgress.map((message, index) => {
                          const isContainer = message.includes('📦');
                          const isAsset = message.includes('📄');
                          const isSuccess = message.includes('✅');
                          const isError = message.includes('❌');
                          const isWarning = message.includes('⚠️');
                          
                          return (
                            <Box key={index} sx={{ 
                              py: 0.5,
                              pl: isAsset ? 3 : isContainer ? 1 : 0,
                              color: isSuccess ? 'success.main' : 
                                     isError ? 'error.main' : 
                                     isWarning ? 'warning.main' :
                                     isContainer ? 'primary.main' : 
                                     isAsset ? 'text.secondary' :
                                     'text.primary',
                              fontWeight: isContainer ? 600 : 'normal',
                              fontSize: isAsset ? '0.8rem' : '0.875rem'
                            }}>
                              {message}
                            </Box>
                          );
                        })}
                      </Box>
                    </Box>
                  )}
                  
                  {/* Retry button - shown after test result */}
                  {testResult && (
                    <Box sx={{ mt: 3, display: 'flex', justifyContent: 'center' }}>
                      <Button
                        variant="outlined"
                        startIcon={<Replay />}
                        onClick={handleTestConnection}
                        disabled={testing}
                        sx={{ minWidth: '150px' }}
                      >
                        {testing ? 'Testing...' : 'Retry Test'}
                      </Button>
                    </Box>
                  )}
                </Box>
              ) : (
                <Button
                  variant="contained"
                  onClick={handleTestConnection}
                  disabled={
                    !connectionType || 
                    !config.name ||
                    (connectionType === 'Service Account' && selectedConnector?.id === 'bigquery' && (!config.projectId || !config.serviceAccount)) ||
                    (connectionType === 'Service Account' && selectedConnector?.id === 'gcs' && (!config.projectId || !config.serviceAccount)) ||
                    (connectionType === 'API Token' && (!config.accountDomain || !config.clientId || !config.clientSecret)) ||
                    (connectionType === 'Access Key' && selectedConnector?.id === 's3' && (!config.accessKeyId || !config.secretAccessKey)) ||
                    (connectionType === 'Account Key' && selectedConnector?.id === 'azure-data-storage' && (!config.accountName || !config.accountKey)) ||
                    (connectionType === 'Connection String' && selectedConnector?.id === 'azure-data-storage' && !config.connectionString)
                  }
                >
                  Test Connection
                </Button>
              )}
            </Box>
          </Box>
        );
      
      case 3:
        return (
          <Box>
            <Typography variant="h6" sx={{ mb: 2 }}>
              Summary
            </Typography>
            <Card variant="outlined" sx={{ p: 2 }}>
              <Typography variant="subtitle1" sx={{ mb: 1 }}>
                <strong>Connector:</strong> {selectedConnector?.name}
              </Typography>
              <Typography variant="subtitle1" sx={{ mb: 1 }}>
                <strong>Connection Type:</strong> {connectionType}
              </Typography>
              <Typography variant="subtitle1" sx={{ mb: 1 }}>
                <strong>Connection Name:</strong> {config.name}
              </Typography>
              {testResult && (
                <Alert severity={testResult.success ? 'success' : 'error'} sx={{ mt: 2 }}>
                  {testResult.message}
                </Alert>
              )}
            </Card>
          </Box>
        );
      
      default:
        return 'Unknown step';
    }
  };

  return (
    <Box sx={{ p: 3 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 4 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
          <Typography variant="h4" component="h1" sx={{ fontWeight: 600 }}>
            Connectors
          </Typography>
        </Box>
        <Box sx={{ display: 'flex', gap: 2 }}>
          <Button
            variant="outlined"
            startIcon={<Refresh />}
            onClick={() => {
              fetchMyConnections();
            }}
            disabled={loading}
          >
            Refresh
          </Button>
          <Button
            variant="contained"
            startIcon={<Add />}
            onClick={() => setDialogOpen(true)}
          >
            New Connector
          </Button>
        </Box>
      </Box>


      {/* My Connections Section */}
      <Card sx={{ mb: 4 }}>
        <CardContent>
          <Box sx={{ display: 'flex', alignItems: 'center', mb: 3 }}>
            <CloudSync sx={{ mr: 1.5, color: 'primary.main', fontSize: 28 }} />
            <Typography variant="h5" sx={{ fontWeight: 600 }}>
              My Connections
            </Typography>
          </Box>
          
          {myConnections.length > 0 ? (
            <Grid container spacing={2}>
                  {myConnections.map((connection) => (
                    <Grid item xs={12} sm={6} md={4} key={connection.id}>
                      <Card variant="outlined" sx={{ p: 2, height: '100%', position: 'relative' }}>
                        <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                          <Avatar sx={{ bgcolor: 'primary.main', mr: 2 }}>
                            <CloudSync />
                          </Avatar>
                          <Box sx={{ flex: 1 }}>
                            <Typography variant="h6" sx={{ fontWeight: 600 }}>
                              {connection.name}
                            </Typography>
                            <Chip
                              label={connection.status}
                              size="small"
                              color={connection.status === 'active' ? 'success' : 'error'}
                              sx={{ mt: 0.5 }}
                            />
                          </Box>
                          <IconButton
                            size="small"
                            onClick={() => handleDeleteClick(connection)}
                            sx={{ 
                              color: 'error.main',
                              '&:hover': {
                                backgroundColor: 'error.light',
                                color: 'error.dark'
                              }
                            }}
                          >
                            <Delete fontSize="small" />
                          </IconButton>
                        </Box>
                        <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
                          Type: {connection.type}
                        </Typography>
                        <Typography variant="body2" color="text.secondary">
                          Last run: {connection.last_run ? new Date(connection.last_run).toLocaleString() : 'Never'}
                        </Typography>
                        {connection.assets_count && (
                          <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
                            Assets: {connection.assets_count}
                          </Typography>
                        )}
                      </Card>
                    </Grid>
                  ))}
            </Grid>
          ) : (
            <Box sx={{ textAlign: 'center', py: 4 }}>
              <CloudSync sx={{ fontSize: 48, color: 'text.secondary', mb: 2, opacity: 0.5 }} />
              <Typography variant="h6" color="text.secondary" sx={{ mb: 1 }}>
                No active connections
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Connect to data sources below to get started
              </Typography>
            </Box>
          )}
        </CardContent>
      </Card>

      {/* Available Connectors Section */}
      <Card>
        <CardContent>
          <Box sx={{ display: 'flex', alignItems: 'center', mb: 3 }}>
            <Add sx={{ mr: 1.5, color: 'primary.main', fontSize: 28 }} />
            <Typography variant="h5" sx={{ fontWeight: 600 }}>
              Available Connectors
            </Typography>
          </Box>
          
          <Grid container spacing={3}>
            {availableConnectors.map((connector) => (
              <Grid item xs={12} sm={4} md={4} key={connector.id}>
                <Card 
                  variant="outlined" 
                  sx={{ 
                    p: 3, 
                    height: '100%',
                    cursor: 'pointer',
                    transition: 'all 0.2s ease-in-out',
                    '&:hover': {
                      boxShadow: 3,
                      transform: 'translateY(-2px)',
                    }
                  }}
                >
                  <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                    <Avatar 
                      sx={{ 
                        bgcolor: connector.color, 
                        mr: 2,
                        width: 48,
                        height: 48,
                      }}
                    >
                      {logoErrors.has(connector.id) ? (
                        <Box sx={{ color: 'white', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                          {connector.fallbackIcon}
                        </Box>
                      ) : (
                        <img 
                          src={connector.logo} 
                          alt={connector.name}
                          style={{ 
                            width: '32px', 
                            height: '32px',
                            objectFit: 'contain'
                          }}
                          onError={() => {
                            setLogoErrors(prev => new Set([...prev, connector.id]));
                          }}
                        />
                      )}
                    </Avatar>
                    <Box>
                      <Typography variant="h6" sx={{ fontWeight: 600 }}>
                        {connector.name}
                      </Typography>
                    </Box>
                  </Box>
                  
                  <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
                    {connector.description}
                  </Typography>
                  
                  <Button
                    variant="contained"
                    fullWidth
                    onClick={() => handleConnectClick(connector)}
                    sx={{
                      bgcolor: connector.color,
                      '&:hover': {
                        bgcolor: connector.color,
                        opacity: 0.9,
                      }
                    }}
                  >
                    Connect
                  </Button>
                </Card>
              </Grid>
            ))}
          </Grid>
        </CardContent>
      </Card>

      {/* New Connector Dialog */}
      <Dialog 
        open={dialogOpen} 
        onClose={() => setDialogOpen(false)}
        maxWidth="sm"
        fullWidth
      >
        <Box sx={{ p: 3, position: 'relative' }}>
          <IconButton 
            onClick={() => setDialogOpen(false)} 
            sx={{ 
              position: 'absolute', 
              right: 8, 
              top: 8 
            }}
          >
            <Close />
          </IconButton>
          
          <Typography variant="h5" sx={{ fontWeight: 600, mb: 3, textAlign: 'center' }}>
            Select Connector
          </Typography>
          
          <Grid container spacing={2}>
            {availableConnectors.map((connector) => (
              <Grid item xs={4} key={connector.id}>
                <Card 
                  variant="outlined" 
                  sx={{ 
                    p: 2,
                    cursor: 'pointer',
                    transition: 'all 0.2s ease-in-out',
                    display: 'flex',
                    flexDirection: 'column',
                    alignItems: 'center',
                    '&:hover': {
                      boxShadow: 3,
                      transform: 'translateY(-2px)',
                      borderColor: connector.color,
                    }
                  }}
                  onClick={() => {
                    handleConnectClick(connector);
                    setDialogOpen(false);
                  }}
                >
                  <Avatar 
                    sx={{ 
                      bgcolor: connector.color, 
                      width: 64,
                      height: 64,
                      mb: 1.5,
                    }}
                  >
                    {logoErrors.has(connector.id) ? (
                      <Box sx={{ color: 'white', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 40 }}>
                        {connector.fallbackIcon}
                      </Box>
                    ) : (
                      <img 
                        src={connector.logo} 
                        alt={connector.name}
                        style={{ 
                          width: '48px', 
                          height: '48px',
                          objectFit: 'contain'
                        }}
                        onError={() => {
                          setLogoErrors(prev => new Set([...prev, connector.id]));
                        }}
                      />
                    )}
                  </Avatar>
                  
                  <Typography 
                    variant="body2" 
                    sx={{ 
                      fontWeight: 600,
                      textAlign: 'center',
                      fontSize: '0.875rem'
                    }}
                  >
                    {connector.name}
                  </Typography>
                </Card>
              </Grid>
            ))}
          </Grid>
        </Box>
      </Dialog>

      {/* Connection Wizard Dialog */}
      <Dialog 
        open={wizardOpen} 
        onClose={handleWizardClose}
        maxWidth="lg"
        fullWidth
      >
        <Box sx={{ p: 3 }}>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
            <Typography variant="h5" sx={{ fontWeight: 600 }}>
              Connect to {selectedConnector?.name}
            </Typography>
            <IconButton onClick={handleWizardClose}>
              <Close />
            </IconButton>
          </Box>
          
          <Stepper activeStep={activeStep} sx={{ mb: 4 }}>
            {wizardSteps.map((label) => (
              <Step key={label}>
                <StepLabel>{label}</StepLabel>
              </Step>
            ))}
          </Stepper>
          
          <Box sx={{ mb: 3, minHeight: 300 }}>
            {renderStepContent(activeStep)}
          </Box>
          
          <Box sx={{ display: 'flex', justifyContent: 'space-between' }}>
            <Button
              disabled={activeStep === 0}
              onClick={handleBack}
              startIcon={<ArrowBack />}
            >
              Back
            </Button>
            <Box>
              <Button onClick={handleWizardClose} sx={{ mr: 1 }}>
                Cancel
              </Button>
              {activeStep === wizardSteps.length - 1 ? (
                <Button variant="contained" onClick={handleWizardClose}>
                  Complete
                </Button>
              ) : (
                <Button
                  variant="contained"
                  onClick={handleNext}
                  endIcon={<ArrowForward />}
                  disabled={
                    (activeStep === 0 && !connectionType) ||
                    (activeStep === 1 && (
                      !config.name || 
                      (connectionType === 'Service Account' && selectedConnector?.id === 'bigquery' && (!config.projectId || !config.serviceAccount)) ||
                      (connectionType === 'Service Account' && selectedConnector?.id === 'gcs' && (!config.projectId || !config.serviceAccount)) ||
                      (connectionType === 'API Token' && selectedConnector?.id === 'starburst' && (!config.accountDomain || !config.clientId || !config.clientSecret)) ||
                      (connectionType === 'Access Key' && selectedConnector?.id === 's3' && (!config.accessKeyId || !config.secretAccessKey)) ||
                      (connectionType === 'Account Key' && selectedConnector?.id === 'azure-data-storage' && (!config.accountName || !config.accountKey)) ||
                      (connectionType === 'Connection String' && selectedConnector?.id === 'azure-data-storage' && !config.connectionString)
                    )) ||
                    (activeStep === 2 && !testResult)
                  }
                >
                  Next
                </Button>
              )}
            </Box>
          </Box>
        </Box>
          </Dialog>

          {/* Delete Confirmation Dialog */}
          <Dialog
            open={deleteDialogOpen}
            onClose={handleDeleteCancel}
            maxWidth="sm"
            fullWidth
          >
            <Box sx={{ p: 3 }}>
              <Typography variant="h6" sx={{ fontWeight: 600, mb: 2 }}>
                Delete Connection
              </Typography>
              
              <Typography variant="body1" sx={{ mb: 3 }}>
                Are you sure you want to delete the connection "{connectionToDelete?.name}"? 
                This action cannot be undone and will also remove all associated discovered assets.
              </Typography>
              
              <Box sx={{ display: 'flex', justifyContent: 'flex-end', gap: 2 }}>
                <Button
                  variant="outlined"
                  onClick={handleDeleteCancel}
                >
                  Cancel
                </Button>
                <Button
                  variant="contained"
                  color="error"
                  onClick={handleDeleteConfirm}
                >
                  Delete
                </Button>
              </Box>
            </Box>
          </Dialog>
        </Box>
      );
    };

    export default ConnectorsPage;
