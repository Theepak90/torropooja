import React, { useState } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  TextField,
  Box,
  Typography,
  Alert,
  Autocomplete,
  Chip,
  IconButton,
  Tabs,
  Tab,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  LinearProgress,
} from '@mui/material';
import {
  Close,
  CloudUpload,
  Add,
  Delete,
  Description,
} from '@mui/icons-material';

const ManualLineageDialog = ({ open, onClose, onSuccess }) => {
  const [activeTab, setActiveTab] = useState('form');
  const [sourceAsset, setSourceAsset] = useState(null);
  const [targetAsset, setTargetAsset] = useState(null);
  const [relationship, setRelationship] = useState('manual');
  const [notes, setNotes] = useState('');
  const [columnMappings, setColumnMappings] = useState([]);
  const [sourceColumn, setSourceColumn] = useState('');
  const [targetColumn, setTargetColumn] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(null);
  const [assets, setAssets] = useState([]);
  const [uploadFile, setUploadFile] = useState(null);
  const [uploadProgress, setUploadProgress] = useState(0);

  React.useEffect(() => {
    if (open) {
      fetchAssets();
    }
  }, [open]);

  const fetchAssets = async () => {
    try {
      const response = await fetch('http://localhost:8099/api/assets?page=0&size=1000');
      const data = await response.json();
      setAssets(data.assets || []);
    } catch (error) {
      console.error('Error fetching assets:', error);
    }
  };

  const handleAddColumnMapping = () => {
    if (sourceColumn && targetColumn) {
      setColumnMappings([...columnMappings, {
        source_column: sourceColumn,
        target_column: targetColumn,
        relationship_type: 'direct_match'
      }]);
      setSourceColumn('');
      setTargetColumn('');
    }
  };

  const handleRemoveColumnMapping = (index) => {
    setColumnMappings(columnMappings.filter((_, i) => i !== index));
  };

  const handleSubmit = async () => {
    if (!sourceAsset || !targetAsset) {
      setError('Please select both source and target assets');
      return;
    }

    setLoading(true);
    setError(null);
    setSuccess(null);

    try {
      const proposalData = {
        source: sourceAsset.id,
        target: targetAsset.id,
        relationship: relationship,
        column_lineage: columnMappings.length > 0 ? columnMappings.map(m => ({
          source_table: sourceAsset.id,
          source_column: m.source_column,
          target_table: targetAsset.id,
          target_column: m.target_column,
          relationship_type: m.relationship_type
        })) : [],
        notes: notes || `Manual lineage entry created via UI`
      };

      console.log('Submitting lineage proposal:', proposalData);

      const url = 'http://localhost:8099/api/lineage/curation/propose';
      console.log('Making request to:', url);
      console.log('Request payload:', proposalData);
      
      let response;
      try {
        response = await fetch(url, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify(proposalData),
          credentials: 'include',
          mode: 'cors'
        });
        console.log('Response received:', response.status, response.statusText);
      } catch (fetchError) {
        // Handle network errors (CORS, connection refused, etc.)
        console.error('Fetch error details:', {
          error: fetchError,
          message: fetchError.message,
          name: fetchError.name,
          stack: fetchError.stack
        });
        throw new Error(`Unable to connect to the server. Please check if the backend is running on http://localhost:8099. Error: ${fetchError.message}`);
      }

      if (!response.ok) {
        let errorMessage = `Server error: ${response.status} ${response.statusText}`;
        try {
          const errorData = await response.json();
          errorMessage = errorData.message || errorData.error || errorMessage;
        } catch (e) {
          // If response is not JSON, try to get text
          try {
            const errorText = await response.text();
            if (errorText) {
              errorMessage = errorText;
            }
          } catch (e2) {
            // Use default error message
          }
        }
        throw new Error(errorMessage);
      }

      const result = await response.json();
      setSuccess('Manual lineage relation created successfully!');
      setTimeout(() => {
        onSuccess && onSuccess();
        handleClose();
      }, 1500);
    } catch (err) {
      console.error('Error submitting lineage proposal:', err);
      setError(err.message || 'Failed to submit lineage proposal. Please check your connection and try again.');
    } finally {
      setLoading(false);
    }
  };

  const handleFileUpload = async () => {
    if (!uploadFile) {
      setError('Please select a file to upload');
      return;
    }

    setLoading(true);
    setError(null);
    setSuccess(null);
    setUploadProgress(0);

    try {
      const formData = new FormData();
      formData.append('file', uploadFile);

      const xhr = new XMLHttpRequest();
      
      xhr.upload.addEventListener('progress', (e) => {
        if (e.lengthComputable) {
          const percentComplete = (e.loaded / e.total) * 100;
          setUploadProgress(percentComplete);
        }
      });

      xhr.addEventListener('load', () => {
        if (xhr.status === 200) {
          const response = JSON.parse(xhr.responseText);
          setSuccess(`Successfully uploaded ${response.proposals_created} lineage proposals!`);
          setTimeout(() => {
            onSuccess && onSuccess();
            handleClose();
          }, 2000);
        } else {
          const errorData = JSON.parse(xhr.responseText);
          setError(errorData.message || 'Upload failed');
        }
        setLoading(false);
      });

      xhr.addEventListener('error', () => {
        setError('Upload failed. Please try again.');
        setLoading(false);
      });

      xhr.open('POST', 'http://localhost:8099/api/lineage/curation/upload');
      xhr.withCredentials = true;
      xhr.send(formData);
    } catch (err) {
      setError(err.message || 'Failed to upload file');
      setLoading(false);
    }
  };

  const handleClose = () => {
    setSourceAsset(null);
    setTargetAsset(null);
    setRelationship('manual');
    setNotes('');
    setColumnMappings([]);
    setSourceColumn('');
    setTargetColumn('');
    setError(null);
    setSuccess(null);
    setUploadFile(null);
    setUploadProgress(0);
    setActiveTab('form');
    onClose();
  };

  const getAssetLabel = (asset) => {
    if (!asset) return '';
    return `${asset.name} (${asset.type}) - ${asset.catalog || 'Unknown'}`;
  };

  return (
    <Dialog open={open} onClose={handleClose} maxWidth="md" fullWidth>
      <DialogTitle>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Description />
            <Typography variant="h6">Manual Lineage Entry</Typography>
          </Box>
          <IconButton onClick={handleClose} size="small">
            <Close />
          </IconButton>
        </Box>
      </DialogTitle>
      
      <DialogContent dividers>
        <Tabs value={activeTab} onChange={(e, v) => setActiveTab(v)} sx={{ mb: 3 }}>
          <Tab label="Form Entry" value="form" />
          <Tab label="CSV/JSON Upload" value="upload" />
        </Tabs>

        {activeTab === 'form' && (
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
            <Autocomplete
              options={assets}
              getOptionLabel={getAssetLabel}
              value={sourceAsset}
              onChange={(e, value) => setSourceAsset(value)}
              renderInput={(params) => (
                <TextField {...params} label="Source Asset" placeholder="Select source table/view" />
              )}
              renderOption={(props, option) => (
                <li {...props}>
                  <Box>
                    <Typography variant="body2" sx={{ fontWeight: 600 }}>
                      {option.name}
                    </Typography>
                    <Typography variant="caption" color="text.secondary">
                      {option.type} • {option.catalog || 'Unknown'}
                    </Typography>
                  </Box>
                </li>
              )}
            />

            <Autocomplete
              options={assets}
              getOptionLabel={getAssetLabel}
              value={targetAsset}
              onChange={(e, value) => setTargetAsset(value)}
              renderInput={(params) => (
                <TextField {...params} label="Target Asset" placeholder="Select target table/view" />
              )}
              renderOption={(props, option) => (
                <li {...props}>
                  <Box>
                    <Typography variant="body2" sx={{ fontWeight: 600 }}>
                      {option.name}
                    </Typography>
                    <Typography variant="caption" color="text.secondary">
                      {option.type} • {option.catalog || 'Unknown'}
                    </Typography>
                  </Box>
                </li>
              )}
            />

            <TextField
              select
              label="Relationship Type"
              value={relationship}
              onChange={(e) => setRelationship(e.target.value)}
              SelectProps={{ native: true }}
            >
              <option value="manual">Manual</option>
              <option value="etl_pipeline">ETL Pipeline</option>
              <option value="elt_pipeline">ELT Pipeline</option>
              <option value="feeds_into">Feeds Into</option>
              <option value="derives_from">Derives From</option>
              <option value="schema_level">Schema Level</option>
            </TextField>

            <Box>
              <Typography variant="subtitle2" sx={{ mb: 1 }}>
                Column Mappings (Optional)
              </Typography>
              <Box sx={{ display: 'flex', gap: 1, mb: 2 }}>
                <TextField
                  size="small"
                  placeholder="Source Column"
                  value={sourceColumn}
                  onChange={(e) => setSourceColumn(e.target.value)}
                  sx={{ flex: 1 }}
                />
                <TextField
                  size="small"
                  placeholder="Target Column"
                  value={targetColumn}
                  onChange={(e) => setTargetColumn(e.target.value)}
                  sx={{ flex: 1 }}
                />
                <Button
                  size="small"
                  variant="outlined"
                  startIcon={<Add />}
                  onClick={handleAddColumnMapping}
                  disabled={!sourceColumn || !targetColumn}
                >
                  Add
                </Button>
              </Box>
              
              {columnMappings.length > 0 && (
                <TableContainer component={Paper} variant="outlined">
                  <Table size="small">
                    <TableHead>
                      <TableRow>
                        <TableCell>Source Column</TableCell>
                        <TableCell>Target Column</TableCell>
                        <TableCell width={80}>Actions</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {columnMappings.map((mapping, index) => (
                        <TableRow key={index}>
                          <TableCell>{mapping.source_column}</TableCell>
                          <TableCell>{mapping.target_column}</TableCell>
                          <TableCell>
                            <IconButton
                              size="small"
                              onClick={() => handleRemoveColumnMapping(index)}
                            >
                              <Delete fontSize="small" />
                            </IconButton>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
              )}
            </Box>

            <TextField
              multiline
              rows={3}
              label="Notes (Optional)"
              value={notes}
              onChange={(e) => setNotes(e.target.value)}
              placeholder="Add any additional notes about this lineage relationship..."
            />
          </Box>
        )}

        {activeTab === 'upload' && (
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            <Alert severity="info">
              Upload a CSV or JSON file containing lineage relationships. CSV format: source_table, target_table, relationship, source_column (optional), target_column (optional), notes (optional)
            </Alert>
            
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
              <Button
                variant="outlined"
                component="label"
                startIcon={<CloudUpload />}
              >
                Select File
                <input
                  type="file"
                  hidden
                  accept=".csv,.json"
                  onChange={(e) => setUploadFile(e.target.files[0])}
                />
              </Button>
              {uploadFile && (
                <Chip
                  label={uploadFile.name}
                  onDelete={() => setUploadFile(null)}
                  color="primary"
                />
              )}
            </Box>

            {uploadProgress > 0 && uploadProgress < 100 && (
              <Box>
                <LinearProgress variant="determinate" value={uploadProgress} />
                <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5 }}>
                  Uploading... {Math.round(uploadProgress)}%
                </Typography>
              </Box>
            )}
          </Box>
        )}

        {error && (
          <Alert severity="error" sx={{ mt: 2 }}>
            {error}
          </Alert>
        )}

        {success && (
          <Alert severity="success" sx={{ mt: 2 }}>
            {success}
          </Alert>
        )}
      </DialogContent>

      <DialogActions>
        <Button onClick={handleClose}>Cancel</Button>
        {activeTab === 'form' ? (
          <Button
            variant="contained"
            onClick={handleSubmit}
            disabled={loading || !sourceAsset || !targetAsset}
          >
            {loading ? 'Creating...' : 'Create Lineage'}
          </Button>
        ) : (
          <Button
            variant="contained"
            onClick={handleFileUpload}
            disabled={loading || !uploadFile}
            startIcon={<CloudUpload />}
          >
            {loading ? 'Uploading...' : 'Upload File'}
          </Button>
        )}
      </DialogActions>
    </Dialog>
  );
};

export default ManualLineageDialog;




