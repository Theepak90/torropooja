import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  Card,
  CardContent,
  Grid,
  Button,
  TextField,
  FormControl,
  FormLabel,
  RadioGroup,
  FormControlLabel,
  Radio,
  Link,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Chip,
  CircularProgress,
  Alert,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  List,
  ListItem,
  ListItemText,
  ListItemSecondaryAction,
  IconButton,
  Snackbar,
  Select,
  MenuItem,
  InputLabel,
  Autocomplete,
  Menu,
  ListItemIcon,
  Tooltip,
} from '@mui/material';
import {
  ArrowBack,
  Search,
  Label,
  TableChart,
  Add,
  Delete,
  Publish,
  AutoAwesome,
  Security,
  ArrowDropDown,
  Folder,
  Schema,
} from '@mui/icons-material';

const MarketplacePage = () => {
  const [resourceType, setResourceType] = useState('GCP');
  const [gcpProject, setGcpProject] = useState('');
  const [dataset, setDataset] = useState('');
  const [tableName, setTableName] = useState('');
  
  // Starburst-specific state variables
  const [catalog, setCatalog] = useState('');
  const [schema, setSchema] = useState('');
  const [loading, setLoading] = useState(false);
  const [tableData, setTableData] = useState(null);
  const [error, setError] = useState(null);
  
  // Tag management states
  const [columnTagDialogOpen, setColumnTagDialogOpen] = useState(false);
  const [tableTagDialogOpen, setTableTagDialogOpen] = useState(false);
  const [catalogTagDialogOpen, setCatalogTagDialogOpen] = useState(false);
  const [schemaTagDialogOpen, setSchemaTagDialogOpen] = useState(false);
  const [selectedColumn, setSelectedColumn] = useState(null);
  const [selectedColumnForTag, setSelectedColumnForTag] = useState('');
  const [newTag, setNewTag] = useState('');
  const [catalogTag, setCatalogTag] = useState(null);
  const [schemaTag, setSchemaTag] = useState(null);
  const [snackbarOpen, setSnackbarOpen] = useState(false);
  const [snackbarMessage, setSnackbarMessage] = useState('');
  const [publishing, setPublishing] = useState(false);
  const [sqlDialogOpen, setSqlDialogOpen] = useState(false);
  const [sqlCommands, setSqlCommands] = useState([]);
  const [billingInfo, setBillingInfo] = useState({ requiresBilling: false, message: '' });
  const [maskedViewSQL, setMaskedViewSQL] = useState('');
  const [maskedViewCreated, setMaskedViewCreated] = useState(false);
  const [maskedViewName, setMaskedViewName] = useState('');
  const [maskedViewError, setMaskedViewError] = useState('');
  const [recommendedTagsDialogOpen, setRecommendedTagsDialogOpen] = useState(false);
  const [recommendedTags, setRecommendedTags] = useState({});
  const [existingTags, setExistingTags] = useState([]);
  const [connectors, setConnectors] = useState([]);
  const [piiDialogOpen, setPiiDialogOpen] = useState(false);
  const [selectedColumnForPii, setSelectedColumnForPii] = useState(null);
  const [tagMenuAnchor, setTagMenuAnchor] = useState(null);

  // Fetch connectors from backend
  const fetchConnectors = async () => {
    try {
      const response = await fetch('http://localhost:8099/api/connectors');
      if (response.ok) {
        const data = await response.json();
        setConnectors(data || []);
      }
    } catch (err) {
      console.error('Error fetching connectors:', err);
    }
  };

  // Load connectors on component mount
  useEffect(() => {
    fetchConnectors();
  }, []);

  // Clear form fields when resource type changes
  useEffect(() => {
    // Clear all form fields when switching resource types
    setGcpProject('');
    setDataset('');
    setTableName('');
    setCatalog('');
    setSchema('');
    setTableData(null);
    setError(null);
    setCatalogTag(null);
    setSchemaTag(null);
  }, [resourceType]);

  // Fetch existing tags
  const fetchExistingTags = async () => {
    try {
      let apiUrl = '';
      if (resourceType === 'GCP') {
        apiUrl = 'http://localhost:8099/api/bigquery/all-tags';
      } else if (resourceType === 'Starburst Galaxy') {
        apiUrl = 'http://localhost:8099/api/starburst/all-tags';
      }
      
      if (apiUrl) {
        const response = await fetch(apiUrl);
        const data = await response.json();
        setExistingTags(data.tags || []);
      }
    } catch (err) {
      console.error('Error fetching existing tags:', err);
    }
  };

  // Helper function to get connector ID for a specific project
  const getConnectorIdForProject = (projectId) => {
    // Find BigQuery connector that matches the project ID
    const bigqueryConnector = connectors.find(connector => 
      connector.type === 'BigQuery' && 
      connector.enabled && 
      connector.config?.project_id === projectId
    );
    return bigqueryConnector ? bigqueryConnector.id : null;
  };

  // Helper function to get Starburst connector ID
  const getStarburstConnectorId = () => {
    // Find any enabled Starburst connector
    const starburstConnector = connectors.find(connector => 
      connector.type === 'Starburst Galaxy' && 
      connector.enabled
    );
    return starburstConnector ? starburstConnector.id : null;
  };

  const handleSearch = async () => {
    // Check required fields based on resource type
    if (resourceType === 'GCP') {
      if (!gcpProject || !dataset || !tableName) {
        setError('Please fill in all required fields');
        return;
      }
    } else if (resourceType === 'Starburst Galaxy') {
      if (!catalog || !schema || !tableName) {
        setError('Please fill in all required fields');
        return;
      }
    } else {
      if (!gcpProject || !dataset || !tableName) {
        setError('Please fill in all required fields');
        return;
      }
    }

    setLoading(true);
    setError(null);
    setTableData(null);

    try {
      // Use the new marketplace search endpoint that fetches from discovered assets
      const requestBody = {
        resourceType: resourceType,
      };

      if (resourceType === 'GCP') {
        requestBody.projectId = gcpProject;
        requestBody.datasetId = dataset;
        requestBody.tableId = tableName;
      } else if (resourceType === 'Starburst Galaxy') {
        requestBody.catalog = catalog;
        requestBody.schema_name = schema;
        requestBody.tableName = tableName;
      } else {
        throw new Error('Unsupported resource type');
      }

      const response = await fetch('http://localhost:8099/api/marketplace/search-table', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(requestBody),
      });

      if (!response.ok) {
        // Try to get error details from the response
        let errorMessage = 'Failed to fetch table details';
        try {
          const errorData = await response.json();
          errorMessage = errorData.error || errorData.detail || errorData.message || `Server error: ${response.status}`;
        } catch (e) {
          errorMessage = `Server error: ${response.status} ${response.statusText}`;
        }
        throw new Error(errorMessage);
      }

      const data = await response.json();
      setTableData(data);
      
      // Set loading to false AFTER setting table data
      setLoading(false);
      
      // Fetch existing tags for autocomplete (don't block on this)
      fetchExistingTags().catch(err => {
        console.error('Error fetching existing tags:', err);
      });
    } catch (err) {
      console.error('API call failed:', err.message);
      setError(`Failed to fetch table details: ${err.message}`);
      setLoading(false);
    }
  };

  // Tag management handlers
  const handleAddColumnTag = (columnName = null) => {
    if (columnName && typeof columnName === 'string') {
      setSelectedColumnForTag(columnName);
    } else {
      setSelectedColumnForTag('');
    }
    setColumnTagDialogOpen(true);
  };

  const handleAddTableTag = () => {
    setTableTagDialogOpen(true);
  };

  const handleAddCatalogTag = () => {
    setCatalogTagDialogOpen(true);
  };

  const handleAddSchemaTag = () => {
    setSchemaTagDialogOpen(true);
  };

  const handleAddTag = () => {
    if (!newTag.trim()) return;

    // Determine which dialog is open and handle accordingly
    if (catalogTagDialogOpen) {
      // Add tag at catalog level - store separately, NOT in columns
      setCatalogTag(newTag.trim());
      setSnackbarMessage(`Tag "${newTag}" will be applied to catalog "${gcpProject}"`);
      setCatalogTagDialogOpen(false);
    } else if (schemaTagDialogOpen) {
      // Add tag at schema level - store separately, NOT in columns
      setSchemaTag(newTag.trim());
      setSnackbarMessage(`Tag "${newTag}" will be applied to schema "${dataset}"`);
      setSchemaTagDialogOpen(false);
    } else if (selectedColumnForTag) {
      // Add tag to specific column
      const updatedColumns = tableData.columns.map(col => 
        col.name === selectedColumnForTag 
          ? { ...col, tags: [...(col.tags || []), newTag.trim()] }
          : col
      );
      setTableData({ ...tableData, columns: updatedColumns });
      setSnackbarMessage(`Tag "${newTag}" added to column "${selectedColumnForTag}"`);
      setColumnTagDialogOpen(false);
    } else {
      // Add tag as table-level tag
      const tableTags = tableData.tableTags || [];
      const updatedTableData = {
        ...tableData,
        tableTags: [...tableTags, newTag.trim()]
      };
      setTableData(updatedTableData);
      setSnackbarMessage(`Tag "${newTag}" added to table "${tableData.tableName}"`);
      setTableTagDialogOpen(false);
    }

    setNewTag('');
    setSelectedColumnForTag('');
    setSnackbarOpen(true);
  };

  const handlePublishTags = async () => {
    if (!tableData) return;

    setPublishing(true);
    console.log(`🚀 Starting publish operation for ${resourceType} table: ${tableName}`);
    console.log(`📊 Publishing ${tableData.columns.length} columns with tags...`);
    
    try {
      let response;
      
      if (resourceType === 'GCP') {
        console.log('📝 Preparing BigQuery publish request...');
        // Prepare the data for BigQuery publishing
        const publishData = {
          projectId: gcpProject,
          datasetId: dataset,
          tableId: tableName,
          columns: tableData.columns.map(col => ({
            name: col.name,
            tags: col.tags || [],
            piiFound: col.piiFound || false,
            piiType: col.piiType || ''
          })),
          tableTags: tableData.tableTags || [],  // Add table-level tags
          connectorId: getConnectorIdForProject(gcpProject)  // Add connector ID
        };

        console.log(`📤 Sending publish request to BigQuery API for ${publishData.projectId}.${publishData.datasetId}.${publishData.tableId}...`);
        response = await fetch('http://localhost:8099/api/bigquery/publish-tags', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify(publishData),
        });
        console.log('📥 BigQuery API response received');
      } else if (resourceType === 'Starburst Galaxy') {
        console.log('📝 Preparing Starburst Galaxy publish request...');
        // Use table-level tags for table tag
        // For now, just send table tag from tableData.tableTags
        const tableTag = tableData.tableTags?.[0] || null;
        
        // Prepare the data for Starburst publishing
        const publishData = {
          catalog: catalog,  // Use catalog state for Starburst catalog
          schema: schema,    // Use schema state for Starburst schema
          tableId: tableName,
          columnTags: tableData.columns.map(col => ({
            columnName: col.name,
            tags: col.tags || [],
            piiFound: col.piiFound || false,
            piiType: col.piiType || ''
          })),
          catalogTag: catalogTag, // Pass catalog-level tag
          schemaTag: schemaTag,    // Pass schema-level tag
          tableTag: tableTag        // Pass table-level tag
        };

        console.log(`📤 Sending publish request to Starburst API for ${publishData.catalog}.${publishData.schema}.${publishData.tableId}...`);
        response = await fetch('http://localhost:8099/api/starburst/publish-tags', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify(publishData),
          signal: AbortSignal.timeout(300000), // 5 minute timeout for Starburst API lookup
        });
        console.log('📥 Starburst API response received');
      } else {
        throw new Error('Unsupported resource type');
      }

      if (!response.ok) {
        // Try to get the detailed error message from the backend
        let errorMessage = `Failed to publish tags to ${resourceType}`;
        let detailedError = null;
        
        try {
          const errorData = await response.json();
          errorMessage = errorData.detail || errorData.message || errorMessage;
          
          // For Starburst, check if it's a table not found error
          if (resourceType === 'Starburst Galaxy' && response.status === 404) {
            detailedError = errorData.detail || errorMessage;
          }
        } catch (e) {
          errorMessage = `HTTP ${response.status}: ${response.statusText}`;
        }
        
        // For Starburst 404 errors, show the detailed error dialog
        if (resourceType === 'Starburst Galaxy' && response.status === 404 && detailedError) {
          setBillingInfo({
            requiresBilling: false,
            message: detailedError
          });
          setSqlDialogOpen(true);
          return; // Don't throw error, just show the dialog
        }
        
        throw new Error(errorMessage);
      }

      const result = await response.json();
      console.log('✅ Publish operation completed:', result);
      
      // Check if the operation was successful
      if (!result.success) {
        console.warn('⚠️ Publish operation failed:', result.message);
        // Handle failure case
        setBillingInfo({
          requiresBilling: result.requiresBilling || false,
          message: result.billingMessage || result.message || 'Operation failed. Please check your configuration.'
        });
        setSqlDialogOpen(true);
      } else {
        console.log('🎉 Tags published successfully! Opening SQL dialog...');
        // Handle success case
        setSqlCommands(result.sqlCommands || []);
        
        // Store masked view information
        setMaskedViewSQL(result.maskedViewSQL || '');
        setMaskedViewCreated(result.maskedViewCreated || false);
        setMaskedViewName(result.maskedViewName || '');
        setMaskedViewError(result.maskedViewError || '');
        
        setBillingInfo({
          requiresBilling: result.requiresBilling || false,
          message: result.billingMessage || 'Operation completed successfully.'
        });
        setSqlDialogOpen(true);
      }
    } catch (err) {
      console.error('Publish failed:', err.message);
      
      // Handle timeout errors
      if (err.name === 'TimeoutError' || err.message.includes('timeout')) {
        setSnackbarMessage('Publishing timed out after 5 minutes. Please check your catalog/schema/table names and try again.');
        setSqlDialogOpen(true);
        setBillingInfo({
          requiresBilling: false,
          message: '❌ Publishing timed out after 5 minutes. This usually means:\n\n1. The catalog/schema/table names are incorrect\n2. The Starburst API is slow or unresponsive\n3. Network connectivity issues\n4. The table lookup is taking too long\n\nPlease verify your catalog, schema, and table names are correct and try again.'
        });
      } else {
        setSnackbarMessage(`Failed to publish tags: ${err.message}`);
        setSnackbarOpen(true);
      }
    } finally {
      setPublishing(false);
    }
  };

  const handleRemoveTag = async (columnName, tagToRemove) => {
    try {
      // Remove from local state first for immediate UI feedback
      const updatedColumns = tableData.columns.map(col => 
        col.name === columnName 
          ? { ...col, tags: col.tags.filter(tag => tag !== tagToRemove) }
          : col
      );
      setTableData({ ...tableData, columns: updatedColumns });
      
      // Call backend to delete the tag from the actual resource
      if (resourceType === 'GCP') {
        // BigQuery delete
        const response = await fetch('http://localhost:8099/api/bigquery/delete-tags', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            projectId: gcpProject,
            datasetId: dataset,
            tableId: tableName,
            columnName: columnName,
            tagToDelete: tagToRemove,
            connectorId: getConnectorIdForProject(gcpProject)  // Add connector ID
          }),
        });
        
        if (!response.ok) {
          throw new Error('Failed to delete tag from BigQuery');
        }
      } else if (resourceType === 'Starburst Galaxy') {
        // Starburst delete - send correct payload format with columnTags
        const response = await fetch('http://localhost:8099/api/starburst/delete-tags', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            catalog: catalog,
            schema: schema,
            tableId: tableName,
            columnTags: [{
              columnName: columnName,
              tags: [tagToRemove]
            }]
          }),
        });
        
        if (!response.ok) {
          throw new Error('Failed to delete tag from Starburst Galaxy');
        }
      }
      
      setSnackbarMessage(`✅ Tag "${tagToRemove}" successfully removed from column "${columnName}"`);
      setSnackbarOpen(true);
    } catch (err) {
      console.error('Delete tag failed:', err.message);
      setSnackbarMessage(`❌ Failed to remove tag: ${err.message}`);
      setSnackbarOpen(true);
      
      // Revert the local change on error by refetching
      handleSearch();
    }
  };

  const handleShowRecommendedTags = () => {
    if (!tableData || !tableData.columns) {
      setSnackbarMessage('No table data available');
      setSnackbarOpen(true);
      return;
    }
    
    // Generate recommended security tags with sensitivity levels (1-5) for PII columns
    const recommendations = {};
    tableData.columns.forEach(col => {
      if (col.piiFound) {
        const name = col.name.toLowerCase();
        const tags = ['PII', 'SENSITIVE', 'DATA_PRIVACY'];
        let sensitivityLevel = 3; // Default medium sensitivity
        
        // Determine sensitivity level and specific tags based on PII type
        if (name.includes('ssn') || name.includes('social') || name.includes('social_security')) {
          sensitivityLevel = 5; // Highest sensitivity
          tags.push('SSN', 'CRITICAL_PII', 'ENCRYPT_AT_REST', 'ENCRYPT_IN_TRANSIT');
        } else if (name.includes('credit') || name.includes('card') || name.includes('payment')) {
          sensitivityLevel = 5; // Highest sensitivity
          tags.push('FINANCIAL', 'PAYMENT_INFO', 'PCI_DSS', 'ENCRYPT_AT_REST', 'ENCRYPT_IN_TRANSIT');
        } else if (name.includes('password') || name.includes('secret') || name.includes('token')) {
          sensitivityLevel = 5; // Highest sensitivity
          tags.push('CREDENTIALS', 'AUTH_TOKEN', 'HASH_AT_REST', 'NEVER_LOG');
        } else if (name.includes('bank') || name.includes('routing') || name.includes('account')) {
          sensitivityLevel = 5; // Highest sensitivity
          tags.push('BANKING_INFO', 'FINANCIAL', 'ENCRYPT_AT_REST');
        } else if (name.includes('date') && (name.includes('birth') || name.includes('dob'))) {
          sensitivityLevel = 4; // High sensitivity
          tags.push('DATE_OF_BIRTH', 'PERSONAL_INFO', 'ENCRYPT_AT_REST');
        } else if (name.includes('address') || name.includes('street') || name.includes('zipcode') || name.includes('postal')) {
          sensitivityLevel = 4; // High sensitivity
          tags.push('ADDRESS', 'LOCATION', 'PERSONAL_INFO', 'ENCRYPT_AT_REST');
        } else if (name.includes('passport') || name.includes('license') || name.includes('national_id')) {
          sensitivityLevel = 5; // Highest sensitivity
          tags.push('GOVERNMENT_ID', 'CRITICAL_PII', 'ENCRYPT_AT_REST', 'ENCRYPT_IN_TRANSIT');
        } else if (name.includes('email') || name.includes('e_mail')) {
          sensitivityLevel = 3; // Medium-high sensitivity
          tags.push('EMAIL', 'PERSONAL_INFO', 'MASK_IN_LOGS');
        } else if (name.includes('phone') || name.includes('mobile') || name.includes('cell')) {
          sensitivityLevel = 3; // Medium-high sensitivity
          tags.push('PHONE', 'PERSONAL_INFO', 'MASK_IN_LOGS');
        } else if (name.includes('name') && (name.includes('first') || name.includes('last') || name.includes('full'))) {
          sensitivityLevel = 2; // Medium sensitivity
          tags.push('NAME', 'PERSONAL_INFO', 'MASK_IN_LOGS');
        } else if (name.includes('id') && (name.includes('user') || name.includes('customer') || name.includes('person'))) {
          sensitivityLevel = 2; // Medium sensitivity
          tags.push('IDENTIFIER', 'PERSONAL_INFO');
        } else {
          sensitivityLevel = 2; // Medium sensitivity for general PII
          tags.push('GENERAL_PII');
        }
        
        // Add ONE PII sensitivity level tag
        tags.push(`PII_SENSITIVITY_LEVEL_${sensitivityLevel}`);
        
        recommendations[col.name] = {
          tags: tags,
          sensitivityLevel: sensitivityLevel
        };
      }
    });
    
    setRecommendedTags(recommendations);
    setRecommendedTagsDialogOpen(true);
  };

  const handleApplyRecommendedTag = (columnName, tag) => {
    // Update the column with the tag
    const updatedColumns = tableData.columns.map(col => 
      col.name === columnName 
        ? { ...col, tags: [...new Set([...(col.tags || []), tag])] }
        : col
    );
    setTableData({ ...tableData, columns: updatedColumns });
    
    // Remove the tag from recommendations
    const updatedRecs = { ...recommendedTags };
    if (updatedRecs[columnName] && updatedRecs[columnName].tags) {
      const rec = updatedRecs[columnName];
      rec.tags = rec.tags.filter(t => t !== tag);
      if (rec.tags.length === 0) {
        delete updatedRecs[columnName];
      }
    }
    setRecommendedTags(updatedRecs);
    
    setSnackbarMessage(`Tag "${tag}" added to column "${columnName}"`);
    setSnackbarOpen(true);
  };

  const handleApplyAllRecommendedTags = () => {
    // Apply all recommended tags
    const updatedColumns = tableData.columns.map(col => {
      const rec = recommendedTags[col.name];
      const recommendations = rec ? rec.tags : [];
      return {
        ...col,
        tags: [...new Set([...(col.tags || []), ...recommendations])]
      };
    });
    
    setTableData({ ...tableData, columns: updatedColumns });
    setRecommendedTags({});
    setRecommendedTagsDialogOpen(false);
    setSnackbarMessage(`All recommended tags added to PII columns`);
    setSnackbarOpen(true);
  };

  const handleOpenTagMenu = (event) => {
    setTagMenuAnchor(event.currentTarget);
  };

  const handleCloseTagMenu = () => {
    setTagMenuAnchor(null);
  };

  const handleTagMenuClick = (action) => {
    handleCloseTagMenu();
    if (action === 'catalog') {
      handleAddCatalogTag();
    } else if (action === 'schema') {
      handleAddSchemaTag();
    } else if (action === 'table') {
      handleAddTableTag();
    } else if (action === 'column') {
      handleAddColumnTag();
    }
  };

  const handleTogglePii = (columnName) => {
    if (!tableData) return;
    
    const column = tableData.columns.find(col => col.name === columnName);
    if (column) {
      setSelectedColumnForPii(column);
      setPiiDialogOpen(true);
    }
  };

  const handleSavePiiChange = () => {
    if (!tableData || !selectedColumnForPii) return;

    const updatedColumns = tableData.columns.map(col => 
      col.name === selectedColumnForPii.name 
        ? { ...col, piiFound: selectedColumnForPii.piiFound, piiType: selectedColumnForPii.piiType || '' }
        : col
    );
    setTableData({ ...tableData, columns: updatedColumns });
    setSnackbarMessage(`✅ PII status updated for column "${selectedColumnForPii.name}"`);
    setSnackbarOpen(true);
    setPiiDialogOpen(false);
    setSelectedColumnForPii(null);
  };

  const handleCloseDialogs = () => {
    setColumnTagDialogOpen(false);
    setTableTagDialogOpen(false);
    setCatalogTagDialogOpen(false);
    setSchemaTagDialogOpen(false);
    setRecommendedTagsDialogOpen(false);
    setPiiDialogOpen(false);
    setSelectedColumn(null);
    setSelectedColumnForTag('');
    setNewTag('');
    setSelectedColumnForPii(null);
  };

  return (
    <Box sx={{ 
      width: '100%',
      minHeight: '100vh',
      p: 3,
      bgcolor: '#fafafa'
    }}>
      <Card sx={{ 
        width: '100%',
        boxShadow: '0 4px 12px rgba(0, 0, 0, 0.1)',
        borderRadius: 2
      }}>
        <CardContent sx={{ p: 4 }}>
          {/* Header */}
          <Typography 
            variant="h4" 
            component="h1" 
            sx={{ 
              fontWeight: 600, 
              color: '#1976d2',
              mb: 4,
              textAlign: 'left'
            }}
          >
            Publish Data Assets to Marketplace
          </Typography>

          {/* Resource Type Selection */}
          <FormControl component="fieldset" sx={{ mb: 4 }}>
            <FormLabel 
              component="legend" 
              sx={{ 
                color: '#1976d2', 
                fontWeight: 600, 
                mb: 2,
                fontSize: '1rem'
              }}
            >
              Resource Type *
            </FormLabel>
            <RadioGroup
              value={resourceType}
              onChange={(e) => setResourceType(e.target.value)}
              row
              sx={{ gap: 2 }}
            >
              <FormControlLabel 
                value="GCP" 
                control={<Radio />} 
                label="GCP" 
                sx={{ 
                  '& .MuiFormControlLabel-label': { 
                    fontWeight: 500,
                    color: '#333'
                  }
                }}
              />
              <FormControlLabel 
                value="Starburst Galaxy" 
                control={<Radio />} 
                label="Starburst Galaxy" 
                sx={{ 
                  '& .MuiFormControlLabel-label': { 
                    fontWeight: 500,
                    color: '#333'
                  }
                }}
              />
            </RadioGroup>
          </FormControl>


          {/* Data Asset Details */}
          <Grid container spacing={3} sx={{ mb: 4 }}>
            {resourceType === 'GCP' && (
              <>
                <Grid item xs={12} sm={5}>
                  <FormControl fullWidth>
                    <Typography 
                      variant="body2" 
                      sx={{ 
                        color: '#1976d2', 
                        fontWeight: 600, 
                        mb: 1,
                        fontSize: '0.875rem'
                      }}
                    >
                      GCP Project *
              </Typography>
                    <TextField
                      fullWidth
                      placeholder="GCP Project"
                      value={gcpProject}
                      onChange={(e) => setGcpProject(e.target.value)}
                      variant="outlined"
                      size="small"
                      style={{ width: '100%', minWidth: '300px' }}
                      sx={{
                        width: '100% !important',
                        minWidth: '300px !important',
                        '& .MuiOutlinedInput-root': {
                          backgroundColor: '#f5f5f5',
                          width: '100% !important',
                          minWidth: '300px !important',
                          '& fieldset': {
                            borderColor: '#e0e0e0',
                          },
                          '&:hover fieldset': {
                            borderColor: '#e0e0e0',
                          },
                          '&.Mui-focused fieldset': {
                            borderColor: '#1976d2',
                          },
                        },
                      }}
                    />
                  </FormControl>
        </Grid>
                <Grid item xs={12} sm={5}>
                  <FormControl fullWidth>
                    <Typography 
                      variant="body2" 
                      sx={{ 
                        color: '#1976d2', 
                        fontWeight: 600, 
                        mb: 1,
                        fontSize: '0.875rem'
                      }}
                    >
                      Dataset *
              </Typography>
                    <TextField
                      fullWidth
                      placeholder="Dataset"
                      value={dataset}
                      onChange={(e) => setDataset(e.target.value)}
                      variant="outlined"
                      size="small"
                      style={{ width: '100%', minWidth: '300px' }}
                      sx={{
                        width: '100% !important',
                        minWidth: '300px !important',
                        '& .MuiOutlinedInput-root': {
                          backgroundColor: '#f5f5f5',
                          width: '100% !important',
                          minWidth: '300px !important',
                          '& fieldset': {
                            borderColor: '#e0e0e0',
                          },
                          '&:hover fieldset': {
                            borderColor: '#e0e0e0',
                          },
                          '&.Mui-focused fieldset': {
                            borderColor: '#1976d2',
                          },
                        },
                      }}
                    />
                  </FormControl>
        </Grid>
                <Grid item xs={12} sm={5}>
                  <FormControl fullWidth>
                    <Typography 
                      variant="body2" 
                      sx={{ 
                        color: '#1976d2', 
                        fontWeight: 600, 
                        mb: 1,
                        fontSize: '0.875rem'
                      }}
                    >
                      Table name *
              </Typography>
                    <TextField
                      fullWidth
                      placeholder="Table"
                      value={tableName}
                      onChange={(e) => setTableName(e.target.value)}
                      variant="outlined"
                      size="small"
                      style={{ width: '100%', minWidth: '300px' }}
                      sx={{
                        width: '100% !important',
                        minWidth: '300px !important',
                        '& .MuiOutlinedInput-root': {
                          backgroundColor: '#f5f5f5',
                          width: '100% !important',
                          minWidth: '300px !important',
                          '& fieldset': {
                            borderColor: '#e0e0e0',
                          },
                          '&:hover fieldset': {
                            borderColor: '#e0e0e0',
                          },
                          '&.Mui-focused fieldset': {
                            borderColor: '#1976d2',
                          },
                        },
                      }}
                    />
                  </FormControl>
        </Grid>
              </>
            )}
            
            {resourceType === 'Starburst Galaxy' && (
              <>
                <Grid item xs={12} sm={5}>
                  <FormControl fullWidth>
                    <Typography 
                      variant="body2" 
                      sx={{ 
                        color: '#1976d2', 
                        fontWeight: 600, 
                        mb: 1,
                        fontSize: '0.875rem'
                      }}
                    >
                      Catalog *
              </Typography>
                    <TextField
                      fullWidth
                      placeholder="Catalog"
                      value={catalog}
                      onChange={(e) => setCatalog(e.target.value)}
                      variant="outlined"
                      size="small"
                      style={{ width: '100%', minWidth: '300px' }}
                      sx={{
                        width: '100% !important',
                        minWidth: '300px !important',
                        '& .MuiOutlinedInput-root': {
                          backgroundColor: '#f5f5f5',
                          width: '100% !important',
                          minWidth: '300px !important',
                          '& fieldset': {
                            borderColor: '#e0e0e0',
                          },
                          '&:hover fieldset': {
                            borderColor: '#e0e0e0',
                          },
                          '&.Mui-focused fieldset': {
                            borderColor: '#1976d2',
                          },
                        },
                      }}
                    />
                  </FormControl>
        </Grid>
                <Grid item xs={12} sm={5}>
                  <FormControl fullWidth>
                    <Typography 
                      variant="body2" 
                      sx={{ 
                        color: '#1976d2', 
                        fontWeight: 600, 
                        mb: 1,
                        fontSize: '0.875rem'
                      }}
                    >
                      Schema *
                    </Typography>
              <TextField
                fullWidth
                      placeholder="Schema"
                      value={schema}
                      onChange={(e) => setSchema(e.target.value)}
                      variant="outlined"
                      size="small"
                      style={{ width: '100%', minWidth: '300px' }}
                      sx={{
                        width: '100% !important',
                        minWidth: '300px !important',
                        '& .MuiOutlinedInput-root': {
                          backgroundColor: '#f5f5f5',
                          width: '100% !important',
                          minWidth: '300px !important',
                          '& fieldset': {
                            borderColor: '#e0e0e0',
                          },
                          '&:hover fieldset': {
                            borderColor: '#e0e0e0',
                          },
                          '&.Mui-focused fieldset': {
                            borderColor: '#1976d2',
                          },
                        },
                      }}
                    />
                  </FormControl>
            </Grid>
                <Grid item xs={12} sm={5}>
              <FormControl fullWidth>
                    <Typography 
                      variant="body2" 
                      sx={{ 
                        color: '#1976d2', 
                        fontWeight: 600, 
                        mb: 1,
                        fontSize: '0.875rem'
                      }}
                    >
                      Table name *
                    </Typography>
                    <TextField
                      fullWidth
                      placeholder="Table"
                      value={tableName}
                      onChange={(e) => setTableName(e.target.value)}
                      variant="outlined"
                      size="small"
                      style={{ width: '100%', minWidth: '300px' }}
                      sx={{
                        width: '100% !important',
                        minWidth: '300px !important',
                        '& .MuiOutlinedInput-root': {
                          backgroundColor: '#f5f5f5',
                          width: '100% !important',
                          minWidth: '300px !important',
                          '& fieldset': {
                            borderColor: '#e0e0e0',
                          },
                          '&:hover fieldset': {
                            borderColor: '#e0e0e0',
                          },
                          '&.Mui-focused fieldset': {
                            borderColor: '#1976d2',
                          },
                        },
                      }}
                    />
              </FormControl>
            </Grid>
              </>
            )}
          </Grid>

          {/* Action Buttons */}
          <Box sx={{ 
            display: 'flex', 
            justifyContent: 'space-between', 
            alignItems: 'center' 
          }}>
            <Button
              variant="outlined"
              startIcon={loading ? <CircularProgress size={20} /> : <Search />}
              onClick={handleSearch}
              disabled={loading}
              sx={{
                borderColor: '#1976d2',
                color: '#1976d2',
                fontWeight: 500,
                px: 3,
                py: 1,
                '&:hover': {
                  borderColor: '#1565c0',
                  backgroundColor: 'rgba(25, 118, 210, 0.04)',
                },
                '&:disabled': {
                  borderColor: '#e0e0e0',
                  color: '#9e9e9e',
                },
              }}
            >
              {loading ? 'Searching...' : 'Search'}
            </Button>
          </Box>

          {/* Error Message */}
          {error && (
            <Alert severity="error" sx={{ mt: 2 }}>
              {error}
            </Alert>
          )}

          {/* Table Results */}
          {tableData && (
            <Box sx={{ mt: 4 }}>
              <Box sx={{ mb: 2 }}>
                <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1 }}>
                  <Typography variant="h6" sx={{ fontWeight: 600 }}>
                    Table: {tableData.tableName}
                  </Typography>
                  <Box sx={{ display: 'flex', gap: 1 }}>
                  <Button
                    variant="outlined"
                    startIcon={<Label />}
                    endIcon={<ArrowDropDown />}
                    onClick={handleOpenTagMenu}
                    sx={{
                      color: '#1976d2',
                      borderColor: '#1976d2',
                      px: 2,
                      py: 1,
                      fontSize: '0.875rem',
                      fontWeight: 500,
                      textTransform: 'none',
                      '&:hover': {
                        backgroundColor: '#f5f5f5',
                        borderColor: '#1976d2',
                      },
                    }}
                  >
                    Add Tags
                  </Button>
                  <Menu
                    anchorEl={tagMenuAnchor}
                    open={Boolean(tagMenuAnchor)}
                    onClose={handleCloseTagMenu}
                  >
                    {resourceType === 'Starburst Galaxy' && (
                      [
                        <MenuItem key="catalog" onClick={() => handleTagMenuClick('catalog')}>
                          <ListItemIcon>
                            <Folder fontSize="small" />
                          </ListItemIcon>
                          <ListItemText>Add Catalog Tags</ListItemText>
                        </MenuItem>,
                        <MenuItem key="schema" onClick={() => handleTagMenuClick('schema')}>
                          <ListItemIcon>
                            <Schema fontSize="small" />
                          </ListItemIcon>
                          <ListItemText>Add Schema Tags</ListItemText>
                        </MenuItem>
                      ]
                    )}
                    <MenuItem onClick={() => handleTagMenuClick('table')}>
                      <ListItemIcon>
                        <Label fontSize="small" />
                      </ListItemIcon>
                      <ListItemText>Add Table Tags</ListItemText>
                    </MenuItem>
                    <MenuItem onClick={() => handleTagMenuClick('column')}>
                      <ListItemIcon>
                        <TableChart fontSize="small" />
                      </ListItemIcon>
                      <ListItemText>Add Column Tags</ListItemText>
                    </MenuItem>
                  </Menu>
                  <Button
                    variant="outlined"
                    startIcon={<Security />}
                    onClick={handleShowRecommendedTags}
                    sx={{
                      color: '#d32f2f',
                      borderColor: '#d32f2f',
                      px: 2,
                      py: 1,
                      fontSize: '0.875rem',
                      fontWeight: 500,
                      textTransform: 'none',
                      '&:hover': {
                        backgroundColor: '#ffebee',
                        borderColor: '#d32f2f',
                      },
                    }}
                  >
                    Recommended PII Tags
                  </Button>
                  <Button
                    variant="contained"
                    startIcon={publishing ? <CircularProgress size={20} /> : <Publish />}
                    onClick={handlePublishTags}
                    disabled={publishing || !tableData}
                    sx={{
                      backgroundColor: '#4caf50',
                      color: 'white',
                      px: 2,
                      py: 1,
                      fontSize: '0.875rem',
                      fontWeight: 500,
                      textTransform: 'none',
                      '&:hover': {
                        backgroundColor: '#45a049',
                      },
                      '&:disabled': {
                        backgroundColor: '#e0e0e0',
                        color: '#9e9e9e',
                      },
                    }}
                  >
                    {publishing ? 'Publishing...' : 'Publish'}
                  </Button>
                </Box>
                </Box>
                
                {/* Table-Level Tags Display */}
                {/* Catalog Tag Display - Only for Starburst */}
                {resourceType === 'Starburst Galaxy' && catalogTag && (
                  <Box sx={{ mb: 2, display: 'flex', gap: 0.5, alignItems: 'center', flexWrap: 'wrap' }}>
                    <Typography variant="body2" sx={{ fontWeight: 600, mr: 1 }}>
                      Catalog Tag:
                    </Typography>
                    <Chip 
                      label={catalogTag} 
                      size="small"
                      onDelete={() => {
                        setCatalogTag(null);
                        setSnackbarMessage(`Tag "${catalogTag}" removed from catalog`);
                        setSnackbarOpen(true);
                      }}
                      deleteIcon={<Delete fontSize="small" />}
                      sx={{
                        backgroundColor: '#e8f5e9',
                        color: '#2e7d32',
                        border: '1px solid #81c784',
                        fontWeight: 600
                      }}
                    />
                  </Box>
                )}

                {/* Schema Tag Display - Only for Starburst */}
                {resourceType === 'Starburst Galaxy' && schemaTag && (
                  <Box sx={{ mb: 2, display: 'flex', gap: 0.5, alignItems: 'center', flexWrap: 'wrap' }}>
                    <Typography variant="body2" sx={{ fontWeight: 600, mr: 1 }}>
                      Schema Tag:
                    </Typography>
                    <Chip 
                      label={schemaTag} 
                      size="small"
                      onDelete={() => {
                        setSchemaTag(null);
                        setSnackbarMessage(`Tag "${schemaTag}" removed from schema`);
                        setSnackbarOpen(true);
                      }}
                      deleteIcon={<Delete fontSize="small" />}
                      sx={{
                        backgroundColor: '#fff3e0',
                        color: '#e65100',
                        border: '1px solid #ffb74d',
                        fontWeight: 600
                      }}
                    />
                  </Box>
                )}

                {(tableData.tableTags && tableData.tableTags.length > 0) && (
                  <Box sx={{ mb: 2, display: 'flex', gap: 0.5, alignItems: 'center', flexWrap: 'wrap' }}>
                    <Typography variant="body2" sx={{ fontWeight: 600, mr: 1 }}>
                      Table Tags:
                    </Typography>
                    {tableData.tableTags.map((tag, idx) => (
                      <Chip 
                        key={idx}
                        label={tag} 
                        size="small"
                        onDelete={() => {
                          const updatedTableTags = tableData.tableTags.filter((_, i) => i !== idx);
                          setTableData({ ...tableData, tableTags: updatedTableTags });
                          setSnackbarMessage(`Tag "${tag}" removed from table`);
                          setSnackbarOpen(true);
                        }}
                        deleteIcon={<Delete fontSize="small" />}
                        sx={{
                          backgroundColor: '#e3f2fd',
                          color: '#1565c0',
                          border: '1px solid #90caf9',
                          fontWeight: 600,
                          fontSize: '0.75rem',
                          '&:hover': {
                            backgroundColor: '#bbdefb',
                          },
                        }}
                        variant="filled"
                      />
                    ))}
                  </Box>
                )}
              </Box>
              <TableContainer component={Paper} sx={{ mt: 2 }}>
            <Table>
              <TableHead>
                    <TableRow sx={{ backgroundColor: '#f5f5f5' }}>
                      <TableCell sx={{ fontWeight: 600 }}>Column Name</TableCell>
                      <TableCell sx={{ fontWeight: 600 }}>Type</TableCell>
                      <TableCell sx={{ fontWeight: 600 }}>Mode</TableCell>
                      <TableCell sx={{ fontWeight: 600 }}>Description</TableCell>
                      <TableCell sx={{ fontWeight: 600 }}>PII Found</TableCell>
                      <TableCell sx={{ fontWeight: 600 }}>Tags</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                    {tableData.columns.map((column, index) => (
                      <TableRow key={index}>
                        <TableCell sx={{ fontWeight: 500 }}>{column.name}</TableCell>
                    <TableCell>
                      <Chip 
                            label={column.type} 
                        size="small" 
                            color="primary" 
                        variant="outlined"
                      />
                    </TableCell>
                    <TableCell>
                      <Chip 
                            label={column.mode} 
                        size="small" 
                            color={column.mode === 'REQUIRED' ? 'error' : 'default'}
                            variant="outlined"
                      />
                    </TableCell>
                        <TableCell>
                          <Typography variant="body2" color={column.description ? 'text.primary' : 'text.secondary'}>
                            {column.description || 'NIL'}
                          </Typography>
                        </TableCell>
                    <TableCell>
                          {column.piiFound ? (
                            <Chip 
                              label={column.piiType || 'PII'} 
                              size="small" 
                              color="error"
                              variant="filled"
                              onClick={() => handleTogglePii(column.name)}
                              sx={{ cursor: 'pointer', '&:hover': { opacity: 0.8 } }}
                            />
                          ) : (
                            <Chip 
                              label="No" 
                              size="small" 
                              color="success"
                              variant="filled"
                              onClick={() => handleTogglePii(column.name)}
                              sx={{ cursor: 'pointer', '&:hover': { opacity: 0.8 } }}
                            />
                          )}
                    </TableCell>
                    <TableCell>
                          <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap', alignItems: 'center' }}>
                            {column.tags && column.tags.length > 0 ? (
                              column.tags.map((tag, tagIndex) => {
                                // Find the corresponding tag detail for this tag
                                const tagDetail = column.tagDetails && column.tagDetails.find(detail => detail.displayName === tag);
                                const tooltipText = tagDetail ? `Tag ID: ${tagDetail.tagId}` : tag;
                                
                                return (
                                  <Tooltip key={tagIndex} title={tooltipText} arrow>
                                    <Chip 
                                      label={tag} 
                                      size="small"
                                      onDelete={() => handleRemoveTag(column.name, tag)}
                                      deleteIcon={<Delete fontSize="small" />}
                                      sx={{
                                        backgroundColor: '#e3f2fd',
                                        color: '#1565c0',
                                        border: '1px solid #90caf9',
                                        fontWeight: 600,
                                        fontSize: '0.75rem',
                                        '&:hover': {
                                          backgroundColor: '#bbdefb',
                                        },
                                        '& .MuiChip-deleteIcon': {
                                          color: '#1565c0',
                                          '&:hover': {
                                            color: '#d32f2f',
                                          }
                                        }
                                      }}
                                      variant="filled"
                                    />
                                  </Tooltip>
                                );
                              })
                            ) : (
                              <Typography 
                                variant="body2" 
                                sx={{ 
                                  color: '#666', 
                                  fontStyle: 'italic',
                                  fontSize: '0.75rem'
                                }}
                              >
                                NIL
                              </Typography>
                            )}
                            <IconButton
                          size="small"
                              onClick={() => handleAddColumnTag(column.name)}
                              sx={{
                                ml: 1,
                                color: '#1976d2',
                                '&:hover': {
                                  backgroundColor: 'rgba(25, 118, 210, 0.04)',
                                }
                              }}
                            >
                              <Add fontSize="small" />
                            </IconButton>
                      </Box>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
            </Box>
          )}

          {/* Column Tag Dialog */}
          <Dialog open={columnTagDialogOpen} onClose={handleCloseDialogs} maxWidth="sm" fullWidth>
            <DialogTitle>
              {selectedColumnForTag && selectedColumnForTag !== '' ? `Add Tag to Column: ${selectedColumnForTag}` : 'Add Tag to Column'}
            </DialogTitle>
            <DialogContent>
              <Box sx={{ mt: 2, display: 'flex', flexDirection: 'column', gap: 2 }}>
                <FormControl fullWidth>
                  <InputLabel>Select Column</InputLabel>
                  <Select
                    value={selectedColumnForTag}
                    onChange={(e) => setSelectedColumnForTag(e.target.value)}
                    label="Select Column"
                  >
                    {tableData?.columns.map((column) => (
                      <MenuItem key={column.name} value={column.name}>
                        {column.name}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
                <Autocomplete
                  freeSolo
                  options={existingTags}
                  value={newTag}
                  onInputChange={(e, newValue) => setNewTag(newValue)}
                  renderInput={(params) => (
                    <TextField
                      {...params}
                      label="Tag Name"
                      placeholder="Enter tag name (e.g., PII, SENSITIVE, REQUIRED)"
                      variant="outlined"
                      onKeyPress={(e) => {
                        if (e.key === 'Enter') {
                          handleAddTag();
                        }
                      }}
                    />
                  )}
                />
              </Box>
            </DialogContent>
            <DialogActions>
              <Button onClick={handleCloseDialogs}>Cancel</Button>
              <Button 
                onClick={handleAddTag} 
                variant="contained"
                disabled={!newTag.trim() || !selectedColumnForTag}
              >
                Add Tag
              </Button>
            </DialogActions>
          </Dialog>

          {/* Table Tag Dialog */}
          <Dialog open={tableTagDialogOpen} onClose={handleCloseDialogs} maxWidth="sm" fullWidth>
            <DialogTitle>{resourceType === 'GCP' ? 'Add Table Tag (BigQuery Label)' : 'Add Tag to All Columns'}</DialogTitle>
            <DialogContent>
              <Box sx={{ mt: 2 }}>
                <Autocomplete
                  freeSolo
                  options={existingTags}
                  value={newTag}
                  onInputChange={(e, newValue) => setNewTag(newValue)}
                  renderInput={(params) => (
                    <TextField
                      {...params}
                      label="Tag Name"
                      placeholder="Enter tag name (e.g., PRODUCTION, ANALYTICS, COMPLIANCE)"
                      variant="outlined"
                      onKeyPress={(e) => {
                        if (e.key === 'Enter') {
                          handleAddTag();
                        }
                      }}
                    />
                  )}
                />
                <Typography variant="body2" sx={{ mt: 1, color: '#666' }}>
                  {resourceType === 'GCP' 
                    ? 'This tag will be added as a BigQuery table label and applied to the table in your project.'
                    : 'This tag will be added to all columns in the table.'}
                </Typography>
              </Box>
            </DialogContent>
            <DialogActions>
              <Button onClick={handleCloseDialogs}>Cancel</Button>
              <Button 
                onClick={handleAddTag} 
                variant="contained"
                disabled={!newTag.trim()}
              >
                {resourceType === 'GCP' ? 'Add Table Tag' : 'Add to All Columns'}
              </Button>
            </DialogActions>
          </Dialog>

          {/* Catalog Tag Dialog - Only for Starburst Galaxy */}
          {resourceType === 'Starburst Galaxy' && (
            <Dialog open={catalogTagDialogOpen} onClose={handleCloseDialogs} maxWidth="sm" fullWidth>
              <DialogTitle>Add Tag to Catalog</DialogTitle>
              <DialogContent>
                <Box sx={{ mt: 2 }}>
                  <Autocomplete
                    freeSolo
                    options={existingTags}
                    value={newTag}
                    onInputChange={(e, newValue) => setNewTag(newValue)}
                    renderInput={(params) => (
                      <TextField
                        {...params}
                        label="Tag Name"
                        placeholder="Enter tag name (e.g., PRODUCTION, ANALYTICS)"
                        variant="outlined"
                        onKeyPress={(e) => {
                          if (e.key === 'Enter') {
                            handleAddTag();
                          }
                        }}
                      />
                    )}
                  />
                  <Typography variant="body2" sx={{ mt: 1, color: '#666' }}>
                    This tag will be added to the entire catalog: {gcpProject}
                  </Typography>
                </Box>
              </DialogContent>
              <DialogActions>
                <Button onClick={handleCloseDialogs}>Cancel</Button>
                <Button 
                  onClick={handleAddTag} 
                  variant="contained"
                  disabled={!newTag.trim()}
                >
                  Add to Catalog
                </Button>
              </DialogActions>
            </Dialog>
          )}

          {/* Schema Tag Dialog - Only for Starburst Galaxy */}
          {resourceType === 'Starburst Galaxy' && (
            <Dialog open={schemaTagDialogOpen} onClose={handleCloseDialogs} maxWidth="sm" fullWidth>
              <DialogTitle>Add Tag to Schema</DialogTitle>
              <DialogContent>
                <Box sx={{ mt: 2 }}>
                  <Autocomplete
                    freeSolo
                    options={existingTags}
                    value={newTag}
                    onInputChange={(e, newValue) => setNewTag(newValue)}
                    renderInput={(params) => (
                      <TextField
                        {...params}
                        label="Tag Name"
                        placeholder="Enter tag name (e.g., PUBLIC, INTERNAL)"
                        variant="outlined"
                        onKeyPress={(e) => {
                          if (e.key === 'Enter') {
                            handleAddTag();
                          }
                        }}
                      />
                    )}
                  />
                  <Typography variant="body2" sx={{ mt: 1, color: '#666' }}>
                    This tag will be added to the schema: {dataset}
                  </Typography>
                </Box>
              </DialogContent>
              <DialogActions>
                <Button onClick={handleCloseDialogs}>Cancel</Button>
                <Button 
                  onClick={handleAddTag} 
                  variant="contained"
                  disabled={!newTag.trim()}
                >
                  Add to Schema
                </Button>
              </DialogActions>
            </Dialog>
          )}

          {/* Recommended Tags Dialog */}
          <Dialog open={recommendedTagsDialogOpen} onClose={handleCloseDialogs} maxWidth="md" fullWidth>
            <DialogTitle>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                  <Security sx={{ color: '#d32f2f', fontSize: 28 }} />
                  <Typography variant="h6">Recommended PII Security Tags</Typography>
                </Box>
                <Button
                  variant="contained"
                  color="error"
                  startIcon={<AutoAwesome />}
                  onClick={handleApplyAllRecommendedTags}
                  size="small"
                  sx={{
                    textTransform: 'none',
                    px: 2,
                  }}
                >
                  Apply All
                </Button>
              </Box>
            </DialogTitle>
            <DialogContent>
              <Box sx={{ mt: 2 }}>
                <Alert severity="warning" sx={{ mb: 2 }}>
                  <Typography variant="body2">
                    These security tags are recommended for columns containing Personally Identifiable Information (PII) with sensitivity levels (1-5). 
                    Tags include classification levels and encryption requirements for proper data governance and compliance.
                  </Typography>
                </Alert>
                
                <Box sx={{ mb: 2, p: 2, backgroundColor: '#f5f5f5', borderRadius: 1 }}>
                  <Typography variant="body2" sx={{ fontWeight: 600, mb: 1 }}>
                    PII Sensitivity Levels:
                  </Typography>
                  <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap' }}>
                    <Chip label="Level 5: Critical (SSN, Credit Cards, Passwords)" size="small" sx={{ backgroundColor: '#ffebee', color: '#c62828', fontWeight: 600 }} />
                    <Chip label="Level 4: High (DOB, Physical Address)" size="small" sx={{ backgroundColor: '#fff3e0', color: '#e65100', fontWeight: 600 }} />
                    <Chip label="Level 3: Medium-High (Email, Phone)" size="small" sx={{ backgroundColor: '#fff9c4', color: '#f57c00', fontWeight: 600 }} />
                    <Chip label="Level 2: Medium (Names, User IDs)" size="small" sx={{ backgroundColor: '#e3f2fd', color: '#1565c0', fontWeight: 600 }} />
                  </Box>
                </Box>
                
                <Typography variant="body2" sx={{ mb: 2, color: '#666', fontWeight: 600 }}>
                  Click on tags to apply them to columns. Click "Apply All" to apply all recommended tags at once.
                </Typography>
                
                <Box sx={{ maxHeight: '400px', overflowY: 'auto' }}>
                  {Object.entries(recommendedTags).map(([columnName, rec]) => {
                    const tags = rec.tags || [];
                    const sensitivityLevel = rec.sensitivityLevel || 0;
                    if (tags.length > 0) {
                      return (
                        <Box key={columnName} sx={{ mb: 3, pb: 2, borderBottom: '1px solid #e0e0e0' }}>
                          <Box sx={{ display: 'flex', alignItems: 'center', mb: 1, gap: 1 }}>
                            <Typography variant="subtitle2" sx={{ fontWeight: 600, color: '#d32f2f' }}>
                              🔒 PII Column: {columnName}
                            </Typography>
                            <Chip 
                              label={`Level ${sensitivityLevel}`} 
                              size="small" 
                              sx={{ 
                                backgroundColor: sensitivityLevel >= 5 ? '#ffebee' : 
                                                sensitivityLevel >= 4 ? '#fff3e0' :
                                                sensitivityLevel >= 3 ? '#fff9c4' : '#e3f2fd',
                                color: sensitivityLevel >= 5 ? '#c62828' :
                                       sensitivityLevel >= 4 ? '#e65100' :
                                       sensitivityLevel >= 3 ? '#f57c00' : '#1565c0',
                                fontWeight: 700,
                                fontSize: '0.7rem'
                              }}
                            />
                          </Box>
                          <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap' }}>
                            {tags.map((tag, idx) => (
                              <Chip
                                key={idx}
                                label={tag}
                                size="small"
                                onClick={() => handleApplyRecommendedTag(columnName, tag)}
                                sx={{
                                  backgroundColor: '#fff3e0',
                                  color: '#f57c00',
                                  border: '1px solid #ffb74d',
                                  fontWeight: 600,
                                  fontSize: '0.75rem',
                                  cursor: 'pointer',
                                  '&:hover': {
                                    backgroundColor: '#ffe0b2',
                                    borderColor: '#ff9800',
                                  },
                                }}
                              />
                            ))}
                          </Box>
                        </Box>
                      );
                    }
                    return null;
                  })}
                  {Object.entries(recommendedTags).length === 0 && (
                    <Typography variant="body2" sx={{ color: '#999', fontStyle: 'italic', textAlign: 'center', py: 4 }}>
                      No PII columns detected. Recommendations are only shown for columns with potentially sensitive data.
                    </Typography>
                  )}
                </Box>
              </Box>
            </DialogContent>
            <DialogActions>
              <Button onClick={handleCloseDialogs}>Close</Button>
            </DialogActions>
          </Dialog>

          {/* PII Status Change Dialog */}
          <Dialog open={piiDialogOpen} onClose={handleCloseDialogs} maxWidth="sm" fullWidth>
            <DialogTitle>
              Change PII Status for Column: {selectedColumnForPii?.name}
            </DialogTitle>
            <DialogContent>
              <Box sx={{ mt: 2, display: 'flex', flexDirection: 'column', gap: 2 }}>
                <FormControl component="fieldset">
                  <FormLabel component="legend">PII Status</FormLabel>
                  <RadioGroup
                    value={selectedColumnForPii?.piiFound ? 'yes' : 'no'}
                    onChange={(e) => {
                      const isPii = e.target.value === 'yes';
                      setSelectedColumnForPii({
                        ...selectedColumnForPii,
                        piiFound: isPii,
                        piiType: isPii ? (selectedColumnForPii?.piiType || 'PII') : ''
                      });
                    }}
                  >
                    <FormControlLabel 
                      value="yes" 
                      control={<Radio />} 
                      label="Mark as PII" 
                    />
                    <FormControlLabel 
                      value="no" 
                      control={<Radio />} 
                      label="Mark as Non-PII" 
                    />
                  </RadioGroup>
                </FormControl>
                {selectedColumnForPii?.piiFound && (
                  <TextField
                    fullWidth
                    label="PII Type (e.g., Email, SSN, Phone)"
                    placeholder="Enter PII type"
                    value={selectedColumnForPii?.piiType || ''}
                    onChange={(e) => {
                      setSelectedColumnForPii({
                        ...selectedColumnForPii,
                        piiType: e.target.value
                      });
                    }}
                    variant="outlined"
                    helperText="Describe the type of PII this column contains"
                  />
                )}
              </Box>
            </DialogContent>
            <DialogActions>
              <Button onClick={handleCloseDialogs}>Cancel</Button>
              <Button 
                onClick={handleSavePiiChange} 
                variant="contained"
              >
                Save Changes
              </Button>
            </DialogActions>
          </Dialog>

          {/* Log Message Dialog */}
          <Dialog open={sqlDialogOpen} onClose={() => setSqlDialogOpen(false)} maxWidth="md" fullWidth>
            <DialogTitle>
              {resourceType === 'GCP' ? 'BigQuery Operation Result' : resourceType === 'Starburst Galaxy' ? 'Starburst Operation Result' : 'Operation Result'}
            </DialogTitle>
            <DialogContent>
              <Box sx={{ mt: 2 }}>
                {/* Show the actual log message */}
                <Alert severity={billingInfo.message?.includes('✅') ? 'success' : 'error'} sx={{ mb: 3 }}>
                  <Typography variant="h6" sx={{ fontWeight: 600, mb: 1 }}>
                    {billingInfo.message?.includes('✅') ? '✅ Operation Successful' : '❌ Operation Failed'}
                  </Typography>
                  <Typography variant="body2" sx={{ mb: 2, fontFamily: 'monospace', fontSize: '0.875rem' }}>
                    {billingInfo.message || 'No log message available. Check the backend logs for details.'}
                  </Typography>
                </Alert>

                {/* Show masked view information for Starburst */}
                {resourceType === 'Starburst Galaxy' && maskedViewSQL && (
                  <Box sx={{ mt: 3 }}>
                    <Typography variant="h6" sx={{ fontWeight: 600, mb: 2 }}>
                      🔒 Masked View Information:
                    </Typography>
                    {maskedViewCreated ? (
                      <Alert severity="success" sx={{ mb: 2 }}>
                        <Typography variant="body2" sx={{ fontWeight: 600, mb: 1 }}>
                          ✅ Masked view created successfully!
                        </Typography>
                        <Typography variant="body2">
                          View Name: <strong>{maskedViewName}</strong>
                        </Typography>
                        <Typography variant="body2" sx={{ mt: 1, mb: 1 }}>
                          The masked view applies different masking levels based on PII sensitivity:
                        </Typography>
                        <Box sx={{ ml: 2 }}>
                          <Typography variant="body2" sx={{ fontSize: '0.875rem' }}>
                            • <strong>CRITICAL PII</strong>: Fully masked (***FULLY_MASKED***)
                          </Typography>
                          <Typography variant="body2" sx={{ fontSize: '0.875rem' }}>
                            • <strong>HIGH PII</strong>: Strong masking (partial info shown)
                          </Typography>
                          <Typography variant="body2" sx={{ fontSize: '0.875rem' }}>
                            • <strong>MEDIUM PII</strong>: Partial masking (some structure preserved)
                          </Typography>
                          <Typography variant="body2" sx={{ fontSize: '0.875rem' }}>
                            • <strong>LOW PII</strong>: Light masking (most data visible)
                          </Typography>
                        </Box>
                      </Alert>
                    ) : maskedViewError ? (
                      <Alert severity="warning" sx={{ mb: 2 }}>
                        <Typography variant="body2" sx={{ fontWeight: 600, mb: 1 }}>
                          ⚠️ Masked view creation failed
                        </Typography>
                        <Typography variant="body2" sx={{ mb: 1 }}>
                          Error: {maskedViewError}
                        </Typography>
                        <Typography variant="body2" sx={{ mb: 1 }}>
                          Different masking levels will be applied based on PII sensitivity.
                        </Typography>
                        <Typography variant="body2">
                          You can manually execute the SQL below to create the masked view.
                        </Typography>
                      </Alert>
                    ) : (
                      <Alert severity="info" sx={{ mb: 2 }}>
                        <Typography variant="body2" sx={{ mb: 1 }}>
                          Masked view SQL generated with different masking levels based on PII sensitivity.
                        </Typography>
                        <Typography variant="body2">
                          You can execute it manually to create the view.
                        </Typography>
                      </Alert>
                    )}
                    
                    <Paper sx={{ p: 2, backgroundColor: '#f5f5f5', maxHeight: '300px', overflow: 'auto', mt: 2 }}>
                      <Typography variant="body2" component="pre" sx={{ 
                        fontFamily: 'monospace', 
                        fontSize: '0.875rem',
                        whiteSpace: 'pre-wrap',
                        wordBreak: 'break-word',
                        backgroundColor: 'white',
                        p: 2,
                        borderRadius: 1,
                        border: '1px solid #e0e0e0'
                      }}>
                        {maskedViewSQL}
                      </Typography>
                    </Paper>
                  </Box>
                )}

                {/* Show SQL commands if available */}
                {sqlCommands.length > 0 && (
                  <Box sx={{ mt: 3 }}>
                    <Typography variant="h6" sx={{ fontWeight: 600, mb: 2 }}>
                      Generated SQL Commands:
                    </Typography>
                    <Paper sx={{ p: 2, backgroundColor: '#f5f5f5', maxHeight: '300px', overflow: 'auto' }}>
                      {sqlCommands.map((command, index) => (
                        <Box key={index} sx={{ mb: 2 }}>
                          <Typography variant="body2" component="pre" sx={{ 
                            fontFamily: 'monospace', 
                            fontSize: '0.875rem',
                            whiteSpace: 'pre-wrap',
                            wordBreak: 'break-word',
                            backgroundColor: 'white',
                            p: 2,
                            borderRadius: 1,
                            border: '1px solid #e0e0e0'
                          }}>
                            {command}
                          </Typography>
                        </Box>
                      ))}
                    </Paper>
                  </Box>
                )}
              </Box>
            </DialogContent>
            <DialogActions>
              <Button onClick={() => setSqlDialogOpen(false)}>
                Close
              </Button>
            </DialogActions>
          </Dialog>

          {/* Snackbar for notifications */}
          <Snackbar
            open={snackbarOpen}
            autoHideDuration={3000}
            onClose={() => setSnackbarOpen(false)}
            message={snackbarMessage}
          />
        </CardContent>
      </Card>
    </Box>
  );
};

export default MarketplacePage;
