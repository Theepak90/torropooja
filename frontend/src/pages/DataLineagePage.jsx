import React, { useState, useEffect, useCallback } from 'react';
import ReactFlow, {
  MiniMap,
  Controls,
  Background,
  useNodesState,
  useEdgesState,
  MarkerType,
  Panel,
  Handle,
  Position,
} from 'reactflow';
import 'reactflow/dist/style.css';
import {
  Box,
  Typography,
  Card,
  CardContent,
  Chip,
  Button,
  CircularProgress,
  Alert,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Grid,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  TextField,
  InputAdornment,
  IconButton,
  Tooltip,
  Autocomplete,
  Tabs,
  Tab,
  Menu,
  Divider,
  List,
  ListItem,
  ListItemText,
  LinearProgress,
  Badge,
} from '@mui/material';
import {
  Refresh,
  ZoomIn,
  ZoomOut,
  FitScreen,
  Info,
  DataObject,
  Search,
  Close,
  AccountTree,
  FilterList,
  TableChart,
  Download,
  Analytics,
  Warning,
  TrendingUp,
  MoreVert,
  CloudDownload,
  Share,
  Add,
  Description,
  ArrowForward,
} from '@mui/icons-material';
import ManualLineageDialog from '../components/ManualLineageDialog';

// Custom node component for better styling
const CustomNode = ({ data }) => {
  const isSelected = data.isSelected;
  const isPipelineNode = data.isPipelineNode || false;
  const pipelineColor = data.pipelineColor || '#666';
  const pipelineLabel = data.pipelineLabel || '';
  
  return (
    <>
      <Handle type="target" position={Position.Left} style={{ background: isPipelineNode ? pipelineColor : '#666', width: 10, height: 10 }} />
      <Handle type="source" position={Position.Right} style={{ background: isPipelineNode ? pipelineColor : '#666', width: 10, height: 10 }} />
      
      <Box
        sx={{
          px: 3,
          py: 2,
          borderRadius: 1,
          border: isPipelineNode ? `3px solid ${pipelineColor}` : '1px solid #ddd',
          backgroundColor: isPipelineNode ? `${pipelineColor}15` : (isSelected ? '#f5f5f5' : '#ffffff'),
          minWidth: 200,
          cursor: 'pointer',
          transition: 'all 0.2s ease',
          boxShadow: isSelected ? '0 0 0 2px #666' : (isPipelineNode ? `0 2px 8px ${pipelineColor}40` : '0 1px 3px rgba(0,0,0,0.1)'),
          '&:hover': {
            boxShadow: isSelected ? '0 0 0 2px #666' : (isPipelineNode ? `0 4px 12px ${pipelineColor}60` : '0 2px 6px rgba(0,0,0,0.15)'),
            transform: 'translateY(-1px)',
          },
        }}
        onClick={() => data.onNodeClick && data.onNodeClick(data.id)}
      >
      {isPipelineNode && pipelineLabel && (
        <Box sx={{ mb: 1 }}>
          <Chip 
            label={pipelineLabel} 
            size="small" 
            sx={{ 
              height: 20, 
              fontSize: 9,
              fontWeight: 700,
              backgroundColor: pipelineColor,
              color: '#ffffff',
              border: 'none'
            }} 
          />
        </Box>
      )}
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
        <DataObject sx={{ fontSize: 18, color: isPipelineNode ? pipelineColor : '#666' }} />
        <Typography variant="body2" sx={{ fontWeight: 600, fontSize: 14, color: '#333' }}>
          {data.name}
        </Typography>
      </Box>
      <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap', alignItems: 'center' }}>
      <Chip 
          label={data.type || 'Unknown'} 
        size="small" 
          variant="outlined"
        sx={{ 
            height: 24, 
          fontSize: 11,
            borderColor: '#ccc',
            color: '#666',
            fontWeight: 500,
            minWidth: '50px'
          }} 
        />
        <Chip 
          label={data.source_system || data.connector_id || 'Unknown'} 
          size="small" 
          variant="outlined"
          sx={{ 
            height: 24, 
            fontSize: 10,
            borderColor: '#999',
            color: '#555',
            fontWeight: 500,
            minWidth: '60px',
            backgroundColor: '#f5f5f5',
            border: '1px solid #ddd'
          }} 
        />
      </Box>
    </Box>
    </>
  );
};

const nodeTypes = {
  custom: CustomNode,
};

// PII Detection Function
const detectPII = (columnName, description) => {
  const piiPatterns = [
    // Email patterns
    /email/i, /e-mail/i, /mail/i,
    // Phone patterns
    /phone/i, /mobile/i, /cell/i, /telephone/i,
    // Name patterns
    /firstname/i, /lastname/i, /fullname/i, /name/i,
    // Address patterns
    /address/i, /street/i, /city/i, /zip/i, /postal/i,
    // ID patterns
    /ssn/i, /social/i, /passport/i, /license/i, /id/i,
    // Financial patterns
    /credit/i, /card/i, /account/i, /bank/i,
    // Personal patterns
    /birth/i, /age/i, /gender/i, /race/i, /ethnicity/i,
    // Location patterns
    /location/i, /gps/i, /coordinate/i, /lat/i, /lng/i,
    // Other sensitive patterns
    /password/i, /secret/i, /private/i, /confidential/i
  ];
  
  const combinedText = `${columnName} ${description || ''}`.toLowerCase();
  return piiPatterns.some(pattern => pattern.test(combinedText));
};

const DataLineagePage = () => {
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [assets, setAssets] = useState([]);
  const [searchTerm, setSearchTerm] = useState('');
  const [filterType, setFilterType] = useState('all');
  const [filterSource, setFilterSource] = useState('all');
  const [columnRelationships, setColumnRelationships] = useState(0);
  const [selectedEdge, setSelectedEdge] = useState(null);
  const [edgeDetailsOpen, setEdgeDetailsOpen] = useState(false);
  const [selectedAssetForLineage, setSelectedAssetForLineage] = useState(null);
  const [fullLineageData, setFullLineageData] = useState({ nodes: [], edges: [] });
  const [showAssetDetails, setShowAssetDetails] = useState(false);
  const [selectedAssetDetails, setSelectedAssetDetails] = useState(null);
  const [activeDetailTab, setActiveDetailTab] = useState('basic');
  const [selectedNode, setSelectedNode] = useState(null);
  const [detailsDialogOpen, setDetailsDialogOpen] = useState(false);
  const [asOf, setAsOf] = useState('');
  const [avgConfidence, setAvgConfidence] = useState(null);
  const [columnPage, setColumnPage] = useState(0);
  const [columnsPerPage] = useState(10);
  const [manualLineageOpen, setManualLineageOpen] = useState(false);

  // Fetch lineage data
  const fetchLineage = async () => {
    try {
      setLoading(true);
      setError(null);
      const url = new URL('http://localhost:8099/api/lineage');
      if (asOf && asOf.trim()) {
        url.searchParams.set('as_of', new Date(asOf).toISOString());
      }
      const response = await fetch(url.toString());
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      const data = await response.json();
      
      console.log('🔵 Lineage API Response:', data);
      console.log('🔵 Number of nodes received:', data.nodes?.length || 0);
      console.log('🔵 Number of edges received:', data.edges?.length || 0);
      console.log('🔵 Column relationships:', data.column_relationships);
      console.log('🔵 Sample nodes:', data.nodes?.slice(0, 3));
      
      setColumnRelationships(data.column_relationships || 0);
      setAvgConfidence(typeof data.avg_confidence === 'number' ? data.avg_confidence : null);
      
      // If no data, clear everything but don't show error yet
      // The error should only show if there are truly no assets at all
      if (!data.nodes || data.nodes.length === 0) {
        setFullLineageData({ nodes: [], edges: [], rawData: { nodes: [], edges: [] } });
        setNodes([]);
        setEdges([]);
        // Don't set error here - let the UI component decide based on whether assets exist
        setError(null);
        return;
      }

      // Layout nodes and DISPLAY ALL BY DEFAULT (no selection required!)
      const layoutedNodes = layoutNodes(data.nodes, data.edges);
      
      // Determine pipeline stage for each node
      const getPipelineStage = (node) => {
        const nameLower = (node.name || '').toLowerCase();
        const catalogLower = (node.catalog || '').toLowerCase();
        const connectorId = node.connector_id || '';
        
        if (connectorId === 'demo_etl_pipeline' || catalogLower.includes('demo')) {
          if (catalogLower.includes('crm') || catalogLower.includes('source')) {
            return { stage: 'source', color: '#9c27b0', label: 'SOURCE' };
          } else if (catalogLower.includes('staging') || nameLower.includes('raw')) {
            return { stage: 'extract', color: '#ff9800', label: 'EXTRACT' };
          } else if (catalogLower.includes('warehouse') || nameLower.includes('dim') || nameLower.includes('fact')) {
            return { stage: 'transform', color: '#2196f3', label: 'TRANSFORM' };
          } else if (catalogLower.includes('analytics') || catalogLower.includes('report')) {
            return { stage: 'load', color: '#4caf50', label: 'LOAD' };
          }
        }
        return null;
      };
      
      const flowNodes = layoutedNodes.map((node, index) => {
        const pipelineInfo = getPipelineStage(node);
        const isPipelineNode = pipelineInfo !== null;
        
        return {
          id: node.id,
          type: 'custom',
          position: node.position,
          sourcePosition: 'right',
          targetPosition: 'left',
          style: isPipelineNode ? {
            border: `3px solid ${pipelineInfo.color}`,
            backgroundColor: `${pipelineInfo.color}15`,
          } : {},
          data: {
            label: node.name,
            name: node.name,
            type: node.type,
            catalog: node.catalog,
            connector_id: node.connector_id,
            source_system: node.source_system,
            id: node.id,
            onNodeClick: handleNodeClick,
            pipelineStage: pipelineInfo?.stage,
            pipelineLabel: pipelineInfo?.label,
            pipelineColor: pipelineInfo?.color,
            isPipelineNode: isPipelineNode,
          },
        };
      });

      // Helper to determine pipeline relationship type
      const getPipelineRelationship = (edge) => {
        const relationship = (edge.relationship || '').toLowerCase();
        if (relationship.includes('extract') || relationship.includes('etl_extract')) {
          return { type: 'extract', color: '#ff9800', label: 'EXTRACT' };
        } else if (relationship.includes('transform') || relationship.includes('etl_transform')) {
          return { type: 'transform', color: '#2196f3', label: 'TRANSFORM' };
        } else if (relationship.includes('load') || relationship.includes('etl_load')) {
          return { type: 'load', color: '#4caf50', label: 'LOAD' };
        }
        return null;
      };
      
      const flowEdges = data.edges.map((edge, index) => {
        const columnCount = edge.column_lineage ? edge.column_lineage.length : 0;
        const pipelineRel = getPipelineRelationship(edge);
        const isPipelineEdge = pipelineRel !== null;
        
        // Determine edge color based on pipeline stage or column lineage
        let edgeColor = '#64b5f6';
        let edgeWidth = 1;
        if (isPipelineEdge) {
          edgeColor = pipelineRel.color;
          edgeWidth = 2.5;
        } else if (columnCount > 0) {
          edgeColor = '#1976d2';
          edgeWidth = 1.5;
        }
        
        const label = isPipelineEdge 
          ? `${pipelineRel.label} (${columnCount} cols)`
          : columnCount > 0 
            ? `${columnCount} columns` 
            : edge.relationship || 'feeds into';
        
        return {
          id: `edge-${index}`,
          source: edge.source,
          target: edge.target,
          type: 'smoothstep',
          animated: isPipelineEdge || columnCount > 0,
          markerEnd: {
            type: MarkerType.ArrowClosed,
            width: isPipelineEdge ? 16 : 12,
            height: isPipelineEdge ? 16 : 12,
            color: edgeColor,
          },
          style: {
            strokeWidth: edgeWidth,
            stroke: edgeColor,
            strokeDasharray: isPipelineEdge ? '0' : (columnCount > 0 ? '0' : '5,5'),
            opacity: isPipelineEdge ? 1 : 0.8,
          },
          label: label,
          labelStyle: { 
            fill: '#ffffff', 
            fontWeight: 600, 
            fontSize: isPipelineEdge ? 12 : 11,
            textShadow: '0 1px 2px rgba(0,0,0,0.5)'
          },
          labelBgStyle: { 
            fill: edgeColor, 
            fillOpacity: 0.95,
            padding: isPipelineEdge ? '6px 10px' : '4px 8px',
            borderRadius: '12px',
            stroke: '#ffffff',
            strokeWidth: 1
          },
          data: {
            column_lineage: edge.column_lineage || [],
            relationship: edge.relationship,
            onEdgeClick: handleEdgeClick,
          },
        };
      });

      // Store data AND DISPLAY ALL ASSETS BY DEFAULT
      setFullLineageData({ nodes: flowNodes, edges: flowEdges, rawData: data });
      setNodes(flowNodes);
      setEdges(flowEdges);
      setError(null);
      
    } catch (error) {
      console.error('Error fetching lineage:', error);
      setError(`Failed to load lineage data: ${error.message}`);
    } finally {
      setLoading(false);
    }
  };

  // Fetch assets for details
  const fetchAssets = async () => {
    try {
      const response = await fetch('http://localhost:8099/api/assets?page=0&size=100');
      const data = await response.json();
      // API returns { assets: [...], pagination: {...} }
      setAssets(Array.isArray(data.assets) ? data.assets : []);
    } catch (error) {
      console.error('Error fetching assets:', error);
      setAssets([]);
    }
  };

  useEffect(() => {
    fetchLineage();
    fetchAssets();
  }, []);


  // Automatic hierarchical layout algorithm
  const layoutNodes = (nodes, edges) => {
    // Create adjacency map for graph traversal
    const adjacencyMap = new Map();
    nodes.forEach(node => adjacencyMap.set(node.id, { node, children: [], parents: [] }));
    
    edges.forEach(edge => {
      const sourceNode = adjacencyMap.get(edge.source);
      const targetNode = adjacencyMap.get(edge.target);
      if (sourceNode && targetNode) {
        sourceNode.children.push(edge.target);
        targetNode.parents.push(edge.source);
      }
    });

    // Find root nodes (nodes with no parents)
    const rootNodes = nodes.filter(node => {
      const nodeData = adjacencyMap.get(node.id);
      return nodeData.parents.length === 0;
    });

    // If no root nodes, use all nodes as potential roots
    const startNodes = rootNodes.length > 0 ? rootNodes : nodes;

    // BFS to assign levels
    const levels = new Map();
    const visited = new Set();
    const queue = startNodes.map(node => ({ id: node.id, level: 0 }));

    while (queue.length > 0) {
      const { id, level } = queue.shift();
      if (visited.has(id)) continue;
      
      visited.add(id);
      levels.set(id, level);

      const nodeData = adjacencyMap.get(id);
      if (nodeData) {
        nodeData.children.forEach(childId => {
          if (!visited.has(childId)) {
            queue.push({ id: childId, level: level + 1 });
          }
        });
      }
    }

    // Handle nodes not reached by BFS
    nodes.forEach(node => {
      if (!levels.has(node.id)) {
        levels.set(node.id, 0);
      }
    });

    // Group nodes by level
    const nodesByLevel = new Map();
    levels.forEach((level, nodeId) => {
      if (!nodesByLevel.has(level)) {
        nodesByLevel.set(level, []);
      }
      nodesByLevel.get(level).push(nodeId);
    });

    // Calculate positions
    const levelSpacing = 250;
    const nodeSpacing = 150;
    const layoutedNodes = [];

    nodes.forEach(node => {
      const level = levels.get(node.id);
      const nodesInLevel = nodesByLevel.get(level);
      const indexInLevel = nodesInLevel.indexOf(node.id);
      
      const x = level * levelSpacing;
      const totalHeight = (nodesInLevel.length - 1) * nodeSpacing;
      const y = indexInLevel * nodeSpacing - totalHeight / 2 + 300;

      layoutedNodes.push({
        ...node,
        position: { x, y },
      });
    });

    return layoutedNodes;
  };

  // Handle node click for popup dialog
  const handleNodeClick = async (nodeId) => {
    try {
      // Reset column pagination when opening a new node
      setColumnPage(0);
      
      // Find asset in local assets array
      const asset = assets.find(a => a.id === nodeId);
      
      let assetData = null;
      if (!asset) {
        // Fetch from API if not in local array
        const response = await fetch(`http://localhost:8099/api/assets/${encodeURIComponent(nodeId)}`);
        if (response.ok) {
          assetData = await response.json();
          setSelectedNode(assetData);
        } else {
          setSelectedNode({ id: nodeId, name: nodeId, error: 'Asset details not found' });
        }
      } else {
        // Fetch detailed info
        const response = await fetch(`http://localhost:8099/api/assets/${encodeURIComponent(nodeId)}`);
        if (response.ok) {
          assetData = await response.json();
          setSelectedNode(assetData);
        } else {
          assetData = asset;
          setSelectedNode(asset);
        }
      }
      
      setDetailsDialogOpen(true);
    } catch (error) {
      console.error('Error fetching node details:', error);
      setSelectedNode({ id: nodeId, name: nodeId, error: error.message });
      setDetailsDialogOpen(true);
    }
  };

  // Handle dialog close
  const handleCloseDialog = () => {
    setDetailsDialogOpen(false);
    setSelectedNode(null);
    setColumnPage(0);  // Reset pagination when closing dialog
  };

  // Handle asset selection for details panel
  const handleAssetDetailsSelection = async (assetId) => {
    if (!assetId) {
      setShowAssetDetails(false);
      setSelectedAssetDetails(null);
      return;
    }

    try {
      // Always fetch full asset details from API to get pipeline_metadata
      const response = await fetch(`http://localhost:8099/api/assets/${encodeURIComponent(assetId)}`);
      if (response.ok) {
        const assetData = await response.json();
        setSelectedAssetDetails(assetData);
        setShowAssetDetails(true);
      } else {
        // Fallback to lineage data if API fails
        const asset = fullLineageData.rawData?.nodes?.find(n => n.id === assetId);
        if (asset) {
          setSelectedAssetDetails(asset);
          setShowAssetDetails(true);
        } else {
          console.error('Asset not found:', assetId);
        }
      }
    } catch (error) {
      console.error('Error fetching asset details:', error);
    }
  };


  const handleEdgeClick = (edgeData) => {
    setSelectedEdge(edgeData);
    setEdgeDetailsOpen(true);
  };

  const handleCloseEdgeDialog = () => {
    setEdgeDetailsOpen(false);
    setSelectedEdge(null);
  };

  const onEdgeClick = (event, edge) => {
    if (edge.data && edge.data.column_lineage) {
      handleEdgeClick(edge.data);
    }
  };

  // Filter lineage for selected asset
  const handleAssetSelection = (assetId) => {
    if (!assetId) {
      // Clear selection - show ALL assets
      setSelectedAssetForLineage(null);
      setNodes(fullLineageData.nodes || []);
      setEdges(fullLineageData.edges || []);
      return;
    }

    setSelectedAssetForLineage(assetId);

    // Use rawData from API response
    const rawNodes = fullLineageData.rawData.nodes;
    const rawEdges = fullLineageData.rawData.edges;

    // Find related nodes (upstream + downstream + selected)
    const relatedNodeIds = new Set([assetId]);
    
    // Find upstream (sources feeding into selected asset)
    const upstreamEdges = rawEdges.filter(e => e.target === assetId);
    upstreamEdges.forEach(edge => {
      relatedNodeIds.add(edge.source);
      // Also add their sources (2 levels up)
      const secondLevelUp = rawEdges.filter(e => e.target === edge.source);
      secondLevelUp.forEach(e2 => relatedNodeIds.add(e2.source));
    });

    // Find downstream (assets depending on selected asset)
    const downstreamEdges = rawEdges.filter(e => e.source === assetId);
    downstreamEdges.forEach(edge => {
      relatedNodeIds.add(edge.target);
      // Also add their targets (2 levels down)
      const secondLevelDown = rawEdges.filter(e => e.source === edge.target);
      secondLevelDown.forEach(e2 => relatedNodeIds.add(e2.target));
    });

    // Filter nodes and edges
    const filteredNodes = rawNodes.filter(n => relatedNodeIds.has(n.id));
    const filteredEdges = rawEdges.filter(e => 
      relatedNodeIds.has(e.source) && relatedNodeIds.has(e.target)
    );

    // Re-layout the filtered graph
    const layoutedNodes = layoutNodes(filteredNodes, filteredEdges);
    const flowNodes = layoutedNodes.map((node) => {
      const originalNode = filteredNodes.find(n => n.id === node.id);
      return {
        id: node.id,
        type: 'custom',
        position: node.position,
        sourcePosition: 'right',
        targetPosition: 'left',
        data: {
          label: node.name,
          name: node.name,
          type: node.type,
          catalog: node.catalog,
          connector_id: node.connector_id,
          source_system: node.source_system,
          id: node.id,
          isSelected: node.id === assetId,
          onNodeClick: handleNodeClick,
        },
      };
    });

    // Create flow edges with proper structure
    const flowEdges = filteredEdges.map((edge, index) => {
      const columnCount = edge.column_lineage ? edge.column_lineage.length : 0;
      const label = columnCount > 0 
        ? `${columnCount} columns` 
        : edge.relationship || 'feeds into';
      
      return {
        id: `${edge.source}->${edge.target}`,
        source: edge.source,
        target: edge.target,
        sourceHandle: null,
        targetHandle: null,
        type: 'smoothstep',
        animated: columnCount > 0,
        markerEnd: {
          type: MarkerType.ArrowClosed,
          width: 12,
          height: 12,
          color: columnCount > 0 ? '#1976d2' : '#64b5f6',
        },
        style: {
          strokeWidth: columnCount > 0 ? 1.5 : 1,
          stroke: columnCount > 0 ? '#1976d2' : '#64b5f6',
          strokeDasharray: columnCount > 0 ? '0' : '5,5',
          opacity: 0.8,
        },
        label: label,
        labelStyle: { 
          fill: '#ffffff', 
          fontWeight: 600, 
          fontSize: 11,
          textShadow: '0 1px 2px rgba(0,0,0,0.3)'
        },
        labelBgStyle: { 
          fill: columnCount > 0 ? '#1976d2' : '#64b5f6', 
          fillOpacity: 0.9,
          padding: '4px 8px',
          borderRadius: '12px',
          stroke: '#ffffff',
          strokeWidth: 1
        },
        data: {
          column_lineage: edge.column_lineage || [],
          relationship: edge.relationship,
          onEdgeClick: handleEdgeClick,
        },
      };
    });

    console.log(`✅ Created ${flowEdges.length} edges for ${flowNodes.length} nodes`);
    console.log('Sample edge:', flowEdges[0]);

    setNodes(flowNodes);
    setEdges(flowEdges);
  };

  // Apply filters
  const filteredNodes = nodes.filter(node => {
    const matchesSearch = !searchTerm || 
      node.data.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
      node.data.catalog.toLowerCase().includes(searchTerm.toLowerCase());
    const matchesType = filterType === 'all' || node.data.type === filterType;
    const matchesSource = filterSource === 'all' || node.data.source_system === filterSource;
    return matchesSearch && matchesType && matchesSource;
  });

  const filteredEdges = edges.filter(edge => {
    const sourceExists = filteredNodes.find(n => n.id === edge.source);
    const targetExists = filteredNodes.find(n => n.id === edge.target);
    return sourceExists && targetExists;
  });

  // Get unique types and sources for filters from full data
  const uniqueTypes = [...new Set(fullLineageData.rawData?.nodes?.map(n => n.type) || [])];
  const uniqueSources = [...new Set(fullLineageData.rawData?.nodes?.map(n => n.source_system) || [])];

  // Dropdown should show ALL assets (only filter by search term if provided)
  // Type and Source filters only apply to the graph visualization, not the dropdown
  const dropdownAssets = fullLineageData.rawData?.nodes?.filter(node => {
    if (!searchTerm) return true;
    return node.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
           node.catalog.toLowerCase().includes(searchTerm.toLowerCase());
  }) || [];

  // Create filtered data for graph visualization (applies ALL filters)
  const filteredRawNodes = fullLineageData.rawData?.nodes?.filter(node => {
    const matchesSearch = !searchTerm || 
      node.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
      node.catalog.toLowerCase().includes(searchTerm.toLowerCase());
    const matchesType = filterType === 'all' || node.type === filterType;
    const matchesSource = filterSource === 'all' || node.source_system === filterSource;
    return matchesSearch && matchesType && matchesSource;
  }) || [];

  return (
    <Box sx={{ minHeight: '120vh', p: 4, pb: 8 }}>
      {/* Header */}
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
        <Box>
          <Typography variant="h4" component="h1" sx={{ fontWeight: 600, fontFamily: 'Comfortaa', display: 'flex', alignItems: 'center', gap: 1 }}>
            <AccountTree sx={{ fontSize: 40, color: '#8FA0F5' }} />
            Data Lineage
          </Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
            Visualize data flow and dependencies across your discovered assets
          </Typography>
        </Box>
        <Box sx={{ display: 'flex', gap: 2 }}>
          <Button
            variant="outlined"
            startIcon={<Add />}
            onClick={() => setManualLineageOpen(true)}
            sx={{ height: 40 }}
          >
            Manual Lineage
          </Button>
          <Button
            variant="contained"
            startIcon={<Refresh />}
            onClick={() => {
              fetchLineage();
            }}
            disabled={loading}
            sx={{ height: 40 }}
          >
            Refresh
          </Button>
        </Box>
      </Box>

      {/* Filters */}
      <Card sx={{ mb: 3 }}>
        <CardContent sx={{ py: 2 }}>
          <Grid container spacing={2} alignItems="center">
            <Grid item xs={12} md={5}>
              <Autocomplete
                options={dropdownAssets.map(n => ({
                  id: n.id,
                  label: `${n.name} (${n.type})`,
                  name: n.name,
                  type: n.type,
                  source: n.source_system,
                }))}
                value={dropdownAssets.find(n => n.id === selectedAssetForLineage) ? {
                  id: selectedAssetForLineage,
                  label: dropdownAssets.find(n => n.id === selectedAssetForLineage)?.name,
                } : null}
                onChange={(event, newValue) => {
                  handleAssetSelection(newValue ? newValue.id : null);
                  handleAssetDetailsSelection(newValue ? newValue.id : null);
                }}
                disabled={!dropdownAssets || dropdownAssets.length === 0}
                renderInput={(params) => (
                  <TextField
                    {...params}
                    size="small"
                    label={`Focus on Asset${dropdownAssets.length > 0 ? ` (${dropdownAssets.length} available)` : ''}`}
                    placeholder="Select an asset to view its lineage..."
                    InputProps={{
                      ...params.InputProps,
                      startAdornment: (
                        <>
                          <InputAdornment position="start">
                            <AccountTree />
                          </InputAdornment>
                          {params.InputProps.startAdornment}
                        </>
                      ),
                    }}
                  />
                )}
                renderOption={(props, option) => (
                  <li {...props}>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, width: '100%' }}>
                      <DataObject sx={{ fontSize: 18, color: '#666' }} />
                      <Box sx={{ flex: 1 }}>
                        <Typography variant="body2" sx={{ fontWeight: 600 }}>
                          {option.name}
                        </Typography>
                        <Typography variant="caption" color="text.secondary">
                          {option.type} • {option.source}
                        </Typography>
                      </Box>
                    </Box>
                  </li>
                )}
              />
            </Grid>
            <Grid item xs={12} md={2}>
              <TextField
                fullWidth
                size="small"
                placeholder="Search..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
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
              <FormControl fullWidth size="small">
                <InputLabel>Type</InputLabel>
                <Select
                  value={filterType}
                  label="Type"
                  onChange={(e) => setFilterType(e.target.value)}
                >
                  <MenuItem value="all">All Types</MenuItem>
                  {uniqueTypes.map(type => (
                    <MenuItem key={type} value={type}>{type}</MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={3}>
              <FormControl fullWidth size="small">
                <InputLabel>Source</InputLabel>
                <Select
                  value={filterSource}
                  label="Source"
                  onChange={(e) => setFilterSource(e.target.value)}
                >
                  <MenuItem value="all">All Sources</MenuItem>
                  {uniqueSources.map(source => (
                    <MenuItem key={source} value={source}>{source}</MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={2}>
              <TextField
                fullWidth
                size="small"
                type="datetime-local"
                label="As of"
                value={asOf}
                onChange={(e) => setAsOf(e.target.value)}
                InputLabelProps={{ shrink: true }}
              />
            </Grid>
            <Grid item xs={12} md={2}>
              <Button
                fullWidth
                variant="outlined"
                startIcon={<FilterList />}
                onClick={() => {
                  setSearchTerm('');
                  setFilterType('all');
                  setFilterSource('all');
                  setAsOf('');
                  handleAssetSelection(null);
                }}
              >
                Clear All
              </Button>
            </Grid>
          </Grid>
        </CardContent>
      </Card>

      {/* Lineage Graph */}
      <Card sx={{ position: 'relative', height: '700px', mb: 4 }}>
        {loading ? (
          <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100%' }}>
            <CircularProgress />
          </Box>
        ) : filteredNodes.length === 0 ? (
          <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100%', p: 4 }}>
            <Alert 
              severity={fullLineageData.rawData?.nodes?.length > 0 ? "info" : "warning"} 
              sx={{ maxWidth: 600 }}
            >
              {fullLineageData.rawData?.nodes?.length > 0 ? (
                <>
                  <Typography variant="h6" gutterBottom>
                    No Assets Match Your Filters
                  </Typography>
                  <Typography variant="body2">
                    Adjust the search term, type, or source filters above to see your data assets.
                  </Typography>
                  <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 2, fontStyle: 'italic' }}>
                    💡 Clear all filters to view all {fullLineageData.rawData?.nodes?.length || 0} available assets.
                  </Typography>
                  <Box sx={{ mt: 3, display: 'flex', gap: 2, flexWrap: 'wrap' }}>
                    <Chip 
                      label={`${fullLineageData.rawData?.nodes?.length || 0} Total Assets`} 
                      color="primary" 
                      size="small"
                      title="Tables and Views available"
                    />
                    <Chip 
                      label={`${columnRelationships} Column Links`} 
                      color="success" 
                      size="small"
                    />
                    {avgConfidence !== null && (
                      <Chip 
                        label={`Avg Confidence ${(avgConfidence * 100).toFixed(1)}%`} 
                        color="default" 
                        size="small"
                        variant="outlined"
                      />
                    )}
                    <Chip 
                      label={`${fullLineageData.rawData?.edges?.length || 0} Relationships`} 
                      color="info" 
                      size="small"
                    />
                  </Box>
                </>
              ) : (
                <>
                  <Typography variant="h6" gutterBottom>
                    No Lineage Data Available
                  </Typography>
                  <Typography variant="body2">
                    No lineage relationships found. This could mean:
                  </Typography>
                  <ul style={{ marginTop: 8 }}>
                    <li>No assets have been discovered yet - run a connector scan</li>
                    <li>Assets exist but have no Views (views define data lineage)</li>
                    <li>Views exist but don't reference other tables/views in their SQL</li>
                    <li>Connectors need to be properly configured</li>
                  </ul>
                  <Typography variant="body2" sx={{ mt: 2, fontStyle: 'italic', color: 'text.secondary' }}>
                    💡 Tip: Discover assets with Views to see lineage relationships!
                  </Typography>
                </>
              )}
            </Alert>
          </Box>
        ) : (
          <ReactFlow
            nodes={filteredNodes}
            edges={filteredEdges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onEdgeClick={onEdgeClick}
            nodeTypes={nodeTypes}
            fitView
            attributionPosition="bottom-left"
          >
            <Background color="#aaa" gap={16} />
            <Controls />
            <MiniMap
              nodeColor={(node) => node.data.type === 'View' ? '#8FA0F5' : '#4caf50'}
              maskColor="rgba(0, 0, 0, 0.1)"
            />
            <Panel position="top-right">
              <Card sx={{ p: 2, backgroundColor: 'rgba(255, 255, 255, 0.95)', maxWidth: 250 }}>
                <Typography variant="body2" sx={{ fontWeight: 600, mb: 1.5 }}>
                  Legend
                </Typography>
                <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1.5 }}>
                  <Box>
                    <Typography variant="caption" sx={{ fontWeight: 600, mb: 0.5, display: 'block' }}>
                      Pipeline Stages
                    </Typography>
                    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5, ml: 1 }}>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <Box sx={{ width: 16, height: 16, backgroundColor: '#9c27b015', border: '2px solid #9c27b0', borderRadius: 0.5 }} />
                        <Typography variant="caption">SOURCE</Typography>
                      </Box>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <Box sx={{ width: 16, height: 16, backgroundColor: '#ff980015', border: '2px solid #ff9800', borderRadius: 0.5 }} />
                        <Typography variant="caption">EXTRACT</Typography>
                      </Box>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <Box sx={{ width: 16, height: 16, backgroundColor: '#2196f315', border: '2px solid #2196f3', borderRadius: 0.5 }} />
                        <Typography variant="caption">TRANSFORM</Typography>
                      </Box>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <Box sx={{ width: 16, height: 16, backgroundColor: '#4caf5015', border: '2px solid #4caf50', borderRadius: 0.5 }} />
                        <Typography variant="caption">LOAD</Typography>
                      </Box>
                    </Box>
                  </Box>
                  <Box sx={{ borderTop: '1px solid #ddd', pt: 1 }}>
                    <Typography variant="caption" sx={{ fontWeight: 600, mb: 0.5, display: 'block' }}>
                      Node Types
                    </Typography>
                    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5, ml: 1 }}>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <Box sx={{ width: 16, height: 16, backgroundColor: '#ffffff', border: '1px solid #ddd', borderRadius: 0.5 }} />
                        <Typography variant="caption">Table</Typography>
                      </Box>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <Box sx={{ width: 16, height: 16, backgroundColor: '#ffffff', border: '1px solid #ddd', borderRadius: 0.5 }} />
                        <Typography variant="caption">View</Typography>
                      </Box>
                    </Box>
                  </Box>
                  <Box sx={{ borderTop: '1px solid #ddd', pt: 1 }}>
                    <Typography variant="caption" sx={{ fontWeight: 600, mb: 0.5, display: 'block' }}>
                      Edge Types
                    </Typography>
                    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5, ml: 1 }}>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <Box sx={{ width: 20, height: 2, backgroundColor: '#ff9800' }} />
                        <Typography variant="caption">EXTRACT Flow</Typography>
                      </Box>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <Box sx={{ width: 20, height: 2, backgroundColor: '#2196f3' }} />
                        <Typography variant="caption">TRANSFORM Flow</Typography>
                      </Box>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <Box sx={{ width: 20, height: 2, backgroundColor: '#4caf50' }} />
                        <Typography variant="caption">LOAD Flow</Typography>
                      </Box>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <Box sx={{ width: 20, height: 2, backgroundColor: '#1976d2' }} />
                        <Typography variant="caption">Column Flow</Typography>
                      </Box>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <Box sx={{ width: 20, height: 2, backgroundColor: '#64b5f6', backgroundImage: 'repeating-linear-gradient(90deg, #64b5f6 0px, #64b5f6 5px, transparent 5px, transparent 10px)' }} />
                        <Typography variant="caption">Data Flow</Typography>
                      </Box>
                    </Box>
                  </Box>
                </Box>
              </Card>
            </Panel>
          </ReactFlow>
        )}
      </Card>

      {/* Asset Details Header */}
      {selectedAssetDetails && (
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3, mt: 4 }}>
          <Typography variant="h5" sx={{ fontWeight: 600, display: 'flex', alignItems: 'center', gap: 1 }}>
            <DataObject sx={{ fontSize: 28, color: '#666' }} />
            Asset Details
          </Typography>
          <Button
            variant="outlined"
            size="small"
            onClick={() => {
              setSelectedAssetDetails(null);
              setSelectedAssetForLineage(null);
              setNodes([]);
              setEdges([]);
              setActiveDetailTab('basic');
            }}
            startIcon={<Close />}
          >
            Clear Selection
          </Button>
        </Box>
      )}

      {/* No Asset Selected State */}
      {!selectedAssetDetails && (
        <Box sx={{ 
          display: 'flex', 
          flexDirection: 'column', 
          alignItems: 'center', 
          justifyContent: 'center', 
          py: 8,
          textAlign: 'center',
          mt: 4
        }}>
          <DataObject sx={{ fontSize: 64, color: '#ddd', mb: 2 }} />
          <Typography variant="h5" color="text.secondary" sx={{ mb: 1 }}>
            No Asset Selected
                  </Typography>
          <Typography variant="body1" color="text.secondary">
            Select an asset from the dropdown above to view its details and lineage
          </Typography>
                  </Box>
      )}

      {/* Asset Details Tabs */}
      {selectedAssetDetails && (
        <Card sx={{ mb: 4, minHeight: '400px' }}>
          <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
            <Tabs 
              value={activeDetailTab} 
              onChange={(e, newValue) => setActiveDetailTab(newValue)}
              variant="scrollable"
              scrollButtons="auto"
            >
              <Tab 
                label="Basic Information" 
                value="basic" 
                icon={<DataObject />}
                iconPosition="start"
                sx={{ textTransform: 'none', fontWeight: 500 }}
              />
              <Tab 
                label="Column Information" 
                value="columns" 
                icon={<TableChart />}
                iconPosition="start"
                sx={{ textTransform: 'none', fontWeight: 500 }}
              />
              <Tab 
                label="Lineage Information" 
                value="lineage" 
                icon={<AccountTree />}
                iconPosition="start"
                sx={{ textTransform: 'none', fontWeight: 500 }}
              />
              <Tab 
                label="Metadata" 
                value="metadata" 
                icon={<Info />}
                iconPosition="start"
                sx={{ textTransform: 'none', fontWeight: 500 }}
              />
              <Tab 
                label="Data Quality" 
                value="quality" 
                icon={<FilterList />}
                iconPosition="start"
                sx={{ textTransform: 'none', fontWeight: 500 }}
              />
              {selectedAssetDetails?.pipeline_metadata?.is_pipeline_asset && (
                <Tab 
                  label="Pipeline Summary" 
                  value="pipeline" 
                  icon={<AccountTree />}
                  iconPosition="start"
                  sx={{ textTransform: 'none', fontWeight: 500 }}
                />
              )}
            </Tabs>
                </Box>
          
          <CardContent sx={{ p: 4, minHeight: '350px', overflow: 'auto' }}>
            {/* Basic Information Tab */}
            {activeDetailTab === 'basic' && (
              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
                <Box>
                  <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 0.5 }}>
                    Asset Name
                    </Typography>
                  <Typography variant="h5" sx={{ fontWeight: 600, color: '#333' }}>
                    {selectedAssetDetails.name}
                    </Typography>
                </Box>
                <Box>
                  <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 0.5 }}>
                      Asset ID
                    </Typography>
                  <Typography variant="body2" sx={{ fontFamily: 'monospace', fontSize: 12, wordBreak: 'break-all', color: '#666' }}>
                    {selectedAssetDetails.id}
                    </Typography>
                </Box>
                <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap' }}>
                  <Chip 
                    label={selectedAssetDetails.type} 
                    size="medium" 
                    variant="outlined"
                    sx={{ height: 32, fontSize: 13, borderColor: '#ccc', color: '#666', fontWeight: 500 }}
                  />
                  <Chip 
                    label={selectedAssetDetails.source_system || selectedAssetDetails.technical_metadata?.source_system || selectedAssetDetails.connector_id || 'Unknown'} 
                    size="medium" 
                    variant="outlined"
                    sx={{ height: 32, fontSize: 13, borderColor: '#ccc', color: '#666', fontWeight: 500 }}
                  />
                </Box>
                <Box>
                  <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 0.5 }}>
                    Catalog
                    </Typography>
                  <Typography variant="body2" sx={{ fontFamily: 'monospace', fontSize: 12, color: '#666' }}>
                    {selectedAssetDetails.catalog || 'N/A'}
                      </Typography>
                </Box>
              </Box>
            )}

            {/* Column Information Tab */}
            {activeDetailTab === 'columns' && (
              <Box>
                <Typography variant="h6" sx={{ fontWeight: 600, mb: 3, color: '#333' }}>
                  Column Information ({selectedAssetDetails.columns?.length || 0} columns)
                </Typography>
                {selectedAssetDetails.columns && selectedAssetDetails.columns.length > 0 ? (
                      <TableContainer component={Paper} variant="outlined">
                    <Table>
                          <TableHead>
                            <TableRow>
                          <TableCell sx={{ fontWeight: 600, backgroundColor: '#f5f5f5' }}>Name</TableCell>
                          <TableCell sx={{ fontWeight: 600, backgroundColor: '#f5f5f5' }}>Type</TableCell>
                          <TableCell sx={{ fontWeight: 600, backgroundColor: '#f5f5f5' }}>PII Status</TableCell>
                          <TableCell sx={{ fontWeight: 600, backgroundColor: '#f5f5f5' }}>Description</TableCell>
                          <TableCell sx={{ fontWeight: 600, backgroundColor: '#f5f5f5' }}>Constraints</TableCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                        {selectedAssetDetails.columns.map((col, index) => {
                          // PII detection logic
                          const isPII = detectPII(col.name, col.description);
                          return (
                              <TableRow key={index}>
                              <TableCell sx={{ fontFamily: 'monospace', fontWeight: 500 }}>
                                {col.name}
                              </TableCell>
                                <TableCell>
                                <Chip 
                                  label={col.type} 
                                  size="small" 
                                  variant="outlined"
                                  sx={{ borderColor: '#ddd', color: '#666' }}
                                />
                                </TableCell>
                              <TableCell>
                                {isPII ? (
                                  <Chip 
                                    label="PII" 
                                    size="small" 
                                    color="error"
                                    sx={{ fontWeight: 600 }}
                                  />
                                ) : (
                                  <Chip 
                                    label="Safe" 
                                    size="small" 
                                    color="success"
                                    variant="outlined"
                                    sx={{ fontWeight: 500 }}
                                  />
                                )}
                              </TableCell>
                              <TableCell sx={{ color: '#666' }}>
                                {col.description || '-'}
                              </TableCell>
                              <TableCell>
                                <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap' }}>
                                  {col.nullable === false && (
                                    <Chip 
                                      label="NOT NULL" 
                                      size="small" 
                                      variant="outlined"
                                      sx={{ fontSize: 9, height: 20, borderColor: '#ff9800', color: '#ff9800' }}
                                    />
                                  )}
                                  {col.unique && (
                                    <Chip 
                                      label="UNIQUE" 
                                      size="small" 
                                      variant="outlined"
                                      sx={{ fontSize: 9, height: 20, borderColor: '#2196f3', color: '#2196f3' }}
                                    />
                                  )}
                                  {col.primary_key && (
                                    <Chip 
                                      label="PK" 
                                      size="small" 
                                      color="primary"
                                      sx={{ fontSize: 9, height: 20 }}
                                    />
                                  )}
                                </Box>
                              </TableCell>
                              </TableRow>
                          );
                        })}
                          </TableBody>
                        </Table>
                      </TableContainer>
                ) : (
                  <Typography variant="body2" color="text.secondary">
                    No column information available
                  </Typography>
                )}
              </Box>
            )}

            {/* Lineage Information Tab */}
            {activeDetailTab === 'lineage' && (
              <Box>
                <Typography variant="h6" sx={{ fontWeight: 600, mb: 3, color: '#333' }}>
                  Lineage Information
                </Typography>
                <Grid container spacing={3}>
                  <Grid item xs={12} md={6}>
                    <Typography variant="subtitle1" sx={{ fontWeight: 600, mb: 2, color: '#333' }}>
                      Upstream Dependencies
                    </Typography>
                    {fullLineageData.rawData?.edges?.filter(e => e.target === selectedAssetDetails.id).length > 0 ? (
                      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                        {fullLineageData.rawData.edges
                          .filter(e => e.target === selectedAssetDetails.id)
                          .map((edge, index) => {
                            const sourceNode = fullLineageData.rawData.nodes.find(n => n.id === edge.source);
                            return (
                              <Box key={index} sx={{ display: 'flex', alignItems: 'center', gap: 1, p: 2, backgroundColor: '#f9f9f9', borderRadius: 1 }}>
                                <Typography variant="body2" sx={{ fontFamily: 'monospace', fontWeight: 500, flex: 1 }}>
                                  {sourceNode?.name || edge.source}
                                </Typography>
                                <Chip 
                                  label={`${edge.column_lineage?.length || 0} cols`}
                                  size="small"
                                  variant="outlined"
                                  sx={{ borderColor: '#ddd', color: '#666' }}
                                />
                              </Box>
                            );
                          })}
                      </Box>
                    ) : (
                      <Typography variant="body2" color="text.secondary" sx={{ fontStyle: 'italic' }}>
                        No upstream dependencies
                        </Typography>
                      )}
                    </Grid>
                  <Grid item xs={12} md={6}>
                    <Typography variant="subtitle1" sx={{ fontWeight: 600, mb: 2, color: '#333' }}>
                      Downstream Dependencies
                    </Typography>
                    {fullLineageData.rawData?.edges?.filter(e => e.source === selectedAssetDetails.id).length > 0 ? (
                      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                        {fullLineageData.rawData.edges
                          .filter(e => e.source === selectedAssetDetails.id)
                          .map((edge, index) => {
                            const targetNode = fullLineageData.rawData.nodes.find(n => n.id === edge.target);
                            return (
                              <Box key={index} sx={{ display: 'flex', alignItems: 'center', gap: 1, p: 2, backgroundColor: '#f9f9f9', borderRadius: 1 }}>
                                <Typography variant="body2" sx={{ fontFamily: 'monospace', fontWeight: 500, flex: 1 }}>
                                  {targetNode?.name || edge.target}
                                </Typography>
                                <Chip 
                                  label={`${edge.column_lineage?.length || 0} cols`}
                                  size="small"
                                  variant="outlined"
                                  sx={{ borderColor: '#ddd', color: '#666' }}
                                />
                              </Box>
                            );
                          })}
                      </Box>
                    ) : (
                      <Typography variant="body2" color="text.secondary" sx={{ fontStyle: 'italic' }}>
                        No downstream dependencies
                      </Typography>
                  )}
                </Grid>
                </Grid>
              </Box>
            )}

            {/* Metadata Tab */}
            {activeDetailTab === 'metadata' && (
              <Box>
                <Typography variant="h6" sx={{ fontWeight: 600, mb: 4, color: '#333' }}>
                  Metadata Information
                </Typography>
                
                {/* Basic Information Cards */}
                <Grid container spacing={3} sx={{ mb: 4 }}>
                  <Grid item xs={12} md={6}>
                    <Box sx={{ p: 3, border: '1px solid #e0e0e0', borderRadius: 2, backgroundColor: '#fafafa' }}>
                      <Typography variant="subtitle1" sx={{ fontWeight: 600, mb: 2, color: '#333' }}>
                        Connection Details
                      </Typography>
                      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                        <Box>
                          <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 0.5 }}>
                            Connector ID
                          </Typography>
                          <Typography variant="body2" sx={{ fontFamily: 'monospace', fontSize: 12, color: '#333', wordBreak: 'break-all' }}>
                            {selectedAssetDetails.connector_id || 'N/A'}
                          </Typography>
                        </Box>
                        <Box>
                          <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 0.5 }}>
                            Source System
                          </Typography>
                          <Typography variant="body2" sx={{ fontSize: 12, color: '#333' }}>
                            {selectedAssetDetails.source_system || selectedAssetDetails.technical_metadata?.source_system || selectedAssetDetails.connector_id || 'Unknown'}
                          </Typography>
                        </Box>
                        <Box>
                          <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 0.5 }}>
                            Status
                          </Typography>
                          <Chip 
                            label="Active" 
                            size="small" 
                            variant="outlined"
                            sx={{ height: 24, fontSize: 11, borderColor: '#4caf50', color: '#4caf50' }}
                          />
                        </Box>
                      </Box>
                    </Box>
                  </Grid>
                  
                  <Grid item xs={12} md={6}>
                    <Box sx={{ p: 3, border: '1px solid #e0e0e0', borderRadius: 2, backgroundColor: '#fafafa' }}>
                      <Typography variant="subtitle1" sx={{ fontWeight: 600, mb: 2, color: '#333' }}>
                        Discovery Information
                      </Typography>
                      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                        <Box>
                          <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 0.5 }}>
                            Last Discovered
                          </Typography>
                          <Typography variant="body2" sx={{ fontSize: 12, color: '#333' }}>
                            {new Date().toLocaleString()}
                          </Typography>
                        </Box>
                        <Box>
                          <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 0.5 }}>
                            Last Modified
                          </Typography>
                          <Typography variant="body2" sx={{ fontSize: 12, color: '#333' }}>
                            {selectedAssetDetails.last_modified ? new Date(selectedAssetDetails.last_modified).toLocaleString() : 'Unknown'}
                          </Typography>
                        </Box>
                        <Box>
                          <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 0.5 }}>
                            Owner
                          </Typography>
                          <Typography variant="body2" sx={{ fontSize: 12, color: '#333' }}>
                            {selectedAssetDetails.owner || 'Unassigned'}
                          </Typography>
                        </Box>
                      </Box>
                    </Box>
                  </Grid>
                </Grid>

                {/* Technical Details */}
                <Box sx={{ mb: 4 }}>
                  <Typography variant="subtitle1" sx={{ fontWeight: 600, mb: 3, color: '#333' }}>
                    Technical Details
                  </Typography>
                  <Grid container spacing={3}>
                    <Grid item xs={12} md={6}>
                      <Box sx={{ p: 2, border: '1px solid #e0e0e0', borderRadius: 1, textAlign: 'center' }}>
                        <Typography variant="body2" color="text.secondary" sx={{ fontSize: 11, fontWeight: 500, mb: 1 }}>
                          Schema
                        </Typography>
                        <Typography variant="body2" sx={{ fontFamily: 'monospace', fontSize: 12, color: '#333' }}>
                          {selectedAssetDetails.schema || selectedAssetDetails.catalog || 'N/A'}
                        </Typography>
                      </Box>
                    </Grid>
                    <Grid item xs={12} md={6}>
                      <Box sx={{ p: 2, border: '1px solid #e0e0e0', borderRadius: 1, textAlign: 'center' }}>
                        <Typography variant="body2" color="text.secondary" sx={{ fontSize: 11, fontWeight: 500, mb: 1 }}>
                          Database
                        </Typography>
                        <Typography variant="body2" sx={{ fontSize: 12, color: '#333' }}>
                          {selectedAssetDetails.database || selectedAssetDetails.source_system || selectedAssetDetails.technical_metadata?.source_system || 'N/A'}
                        </Typography>
                      </Box>
                    </Grid>
                    <Grid item xs={12} md={6}>
                      <Box sx={{ p: 2, border: '1px solid #e0e0e0', borderRadius: 1, textAlign: 'center' }}>
                        <Typography variant="body2" color="text.secondary" sx={{ fontSize: 11, fontWeight: 500, mb: 1 }}>
                          Row Count
                        </Typography>
                        <Typography variant="body2" sx={{ fontSize: 12, color: '#333' }}>
                          {selectedAssetDetails.row_count ? selectedAssetDetails.row_count.toLocaleString() : 'Unknown'}
                        </Typography>
                      </Box>
                    </Grid>
                    <Grid item xs={12} md={6}>
                      <Box sx={{ p: 2, border: '1px solid #e0e0e0', borderRadius: 1, textAlign: 'center' }}>
                        <Typography variant="body2" color="text.secondary" sx={{ fontSize: 11, fontWeight: 500, mb: 1 }}>
                          Size
                        </Typography>
                        <Typography variant="body2" sx={{ fontSize: 12, color: '#333' }}>
                          {selectedAssetDetails.size ? `${(selectedAssetDetails.size / 1024 / 1024).toFixed(2)} MB` : 'Unknown'}
                        </Typography>
                      </Box>
                    </Grid>
                  </Grid>
                </Box>

                {/* Data Governance */}
                <Box sx={{ mb: 4 }}>
                  <Typography variant="subtitle1" sx={{ fontWeight: 600, mb: 3, color: '#333' }}>
                    Data Governance
                  </Typography>
                  <Grid container spacing={3}>
                    <Grid item xs={12} md={4}>
                      <Box sx={{ p: 2, border: '1px solid #e0e0e0', borderRadius: 1, textAlign: 'center' }}>
                        <Typography variant="body2" color="text.secondary" sx={{ fontSize: 11, fontWeight: 500, mb: 1 }}>
                          Classification
                        </Typography>
                        <Chip 
                          label={selectedAssetDetails.classification || 'Unclassified'} 
                          size="small" 
                          variant="outlined"
                          sx={{ height: 24, fontSize: 11, borderColor: '#666', color: '#666' }}
                        />
                      </Box>
                    </Grid>
                    <Grid item xs={12} md={4}>
                      <Box sx={{ p: 2, border: '1px solid #e0e0e0', borderRadius: 1, textAlign: 'center' }}>
                        <Typography variant="body2" color="text.secondary" sx={{ fontSize: 11, fontWeight: 500, mb: 1 }}>
                          Retention Policy
                        </Typography>
                        <Typography variant="body2" sx={{ fontSize: 12, color: '#333' }}>
                          {selectedAssetDetails.retention_policy || 'No policy defined'}
                        </Typography>
                      </Box>
                    </Grid>
                    <Grid item xs={12} md={4}>
                      <Box sx={{ p: 2, border: '1px solid #e0e0e0', borderRadius: 1, textAlign: 'center' }}>
                        <Typography variant="body2" color="text.secondary" sx={{ fontSize: 11, fontWeight: 500, mb: 1 }}>
                          Data Quality
                        </Typography>
                        <Typography variant="body2" sx={{ fontSize: 12, color: '#333' }}>
                          {selectedAssetDetails.columns ? Math.round((selectedAssetDetails.columns.filter(col => col.description && col.description !== '-').length / selectedAssetDetails.columns.length) * 100) : 0}% Complete
                        </Typography>
                      </Box>
                    </Grid>
                  </Grid>
                </Box>

                {/* PII Analysis Summary */}
                <Box>
                  <Typography variant="subtitle1" sx={{ fontWeight: 600, mb: 3, color: '#333' }}>
                    PII Analysis Summary
                  </Typography>
                  <Grid container spacing={3}>
                    <Grid item xs={12} md={4}>
                      <Box sx={{ textAlign: 'center', p: 3, border: '1px solid #e0e0e0', borderRadius: 2, backgroundColor: '#fafafa' }}>
                        <Typography variant="h4" sx={{ fontWeight: 600, color: '#333', mb: 1 }}>
                          {selectedAssetDetails.columns ? selectedAssetDetails.columns.filter(col => detectPII(col.name, col.description)).length : 0}
                        </Typography>
                        <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500 }}>
                          PII Columns
                        </Typography>
                      </Box>
                    </Grid>
                    <Grid item xs={12} md={4}>
                      <Box sx={{ textAlign: 'center', p: 3, border: '1px solid #e0e0e0', borderRadius: 2, backgroundColor: '#fafafa' }}>
                        <Typography variant="h4" sx={{ fontWeight: 600, color: '#333', mb: 1 }}>
                          {selectedAssetDetails.columns ? selectedAssetDetails.columns.length - selectedAssetDetails.columns.filter(col => detectPII(col.name, col.description)).length : 0}
                        </Typography>
                        <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500 }}>
                          Safe Columns
                        </Typography>
                      </Box>
                    </Grid>
                    <Grid item xs={12} md={4}>
                      <Box sx={{ textAlign: 'center', p: 3, border: '1px solid #e0e0e0', borderRadius: 2, backgroundColor: '#fafafa' }}>
                        <Typography variant="h4" sx={{ fontWeight: 600, color: '#333', mb: 1 }}>
                          {selectedAssetDetails.columns ? Math.round((selectedAssetDetails.columns.filter(col => detectPII(col.name, col.description)).length / selectedAssetDetails.columns.length) * 100) : 0}%
                        </Typography>
                        <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500 }}>
                          PII Percentage
                        </Typography>
                      </Box>
                    </Grid>
                  </Grid>
                </Box>
              </Box>
            )}

            {/* Data Quality Tab */}
            {activeDetailTab === 'quality' && (
              <Box>
                <Typography variant="h6" sx={{ fontWeight: 600, mb: 3, color: '#333' }}>
                  Data Quality Metrics
                </Typography>
                <Grid container spacing={3}>
                  <Grid item xs={12} md={4}>
                    <Box sx={{ textAlign: 'center', p: 2, backgroundColor: '#f9f9f9', borderRadius: 1 }}>
                      <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 1 }}>
                        Column Completeness
                      </Typography>
                      <Typography variant="h4" sx={{ fontWeight: 600, color: '#4caf50' }}>
                        {selectedAssetDetails.columns ? Math.round((selectedAssetDetails.columns.filter(col => col.description && col.description !== '-').length / selectedAssetDetails.columns.length) * 100) : 0}%
                      </Typography>
                    </Box>
                  </Grid>
                  <Grid item xs={12} md={4}>
                    <Box sx={{ textAlign: 'center', p: 2, backgroundColor: '#f9f9f9', borderRadius: 1 }}>
                      <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 1 }}>
                        Lineage Coverage
                      </Typography>
                      <Typography variant="h4" sx={{ fontWeight: 600, color: '#1976d2' }}>
                        {fullLineageData.rawData?.edges?.filter(e => e.source === selectedAssetDetails.id || e.target === selectedAssetDetails.id).length || 0}
                      </Typography>
                      <Typography variant="body2" color="text.secondary" sx={{ fontSize: 11 }}>
                        connections
                      </Typography>
                    </Box>
                  </Grid>
                  <Grid item xs={12} md={4}>
                    <Box sx={{ textAlign: 'center', p: 2, backgroundColor: '#f9f9f9', borderRadius: 1 }}>
                      <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 1 }}>
                        Data Types
                      </Typography>
                      <Typography variant="h4" sx={{ fontWeight: 600, color: '#ff9800' }}>
                        {selectedAssetDetails.columns ? [...new Set(selectedAssetDetails.columns.map(col => col.type))].length : 0}
                      </Typography>
                      <Typography variant="body2" color="text.secondary" sx={{ fontSize: 11 }}>
                        unique types
                      </Typography>
                    </Box>
                  </Grid>
                  <Grid item xs={12}>
                    <Box>
                      <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 1 }}>
                        Data Types Used
                      </Typography>
                      <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap' }}>
                        {selectedAssetDetails.columns ? 
                          [...new Set(selectedAssetDetails.columns.map(col => col.type))].map((type, index) => (
                            <Chip 
                              key={index}
                              label={type} 
                              size="small" 
                              variant="outlined"
                              sx={{ borderColor: '#ddd', color: '#666' }}
                            />
                          )) : null
                        }
                      </Box>
                    </Box>
                  </Grid>
                </Grid>
              </Box>
            )}

            {/* Pipeline Summary Tab - Only for ETL/ELT Pipelines */}
            {activeDetailTab === 'pipeline' && selectedAssetDetails?.pipeline_metadata?.is_pipeline_asset && (
              <Box>
                <Typography variant="h6" sx={{ fontWeight: 600, mb: 3, color: '#333' }}>
                  Pipeline Summary
                </Typography>
                
                {/* Pipeline Overview */}
                <Box sx={{ mb: 4 }}>
                  <Card sx={{ bgcolor: '#f5f5f5', border: '2px solid #1976d2', borderRadius: 2 }}>
                    <CardContent>
                      <Grid container spacing={3}>
                        <Grid item xs={12} md={6}>
                          <Box>
                            <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 1 }}>
                              Pipeline Name
                            </Typography>
                            <Typography variant="h6" sx={{ fontWeight: 600, color: '#1976d2' }}>
                              {selectedAssetDetails.pipeline_metadata.pipeline_name || 'ETL/ELT Pipeline'}
                            </Typography>
                          </Box>
                        </Grid>
                        <Grid item xs={12} md={6}>
                          <Box>
                            <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 1 }}>
                              Pipeline Type
                            </Typography>
                            <Chip 
                              label={selectedAssetDetails.pipeline_metadata.pipeline_type || 'ETL'} 
                              color="primary" 
                              sx={{ fontWeight: 600, fontSize: 14 }}
                            />
                          </Box>
                        </Grid>
                        <Grid item xs={12} md={6}>
                          <Box>
                            <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 1 }}>
                              Current Stage
                            </Typography>
                            <Chip 
                              label={selectedAssetDetails.pipeline_metadata.pipeline_stage?.toUpperCase() || 'UNKNOWN'} 
                              color="success" 
                              sx={{ fontWeight: 600, fontSize: 13 }}
                            />
                          </Box>
                        </Grid>
                        <Grid item xs={12} md={6}>
                          <Box>
                            <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 1 }}>
                              Total Pipeline Stages
                            </Typography>
                            <Typography variant="h6" sx={{ fontWeight: 600 }}>
                              {selectedAssetDetails.pipeline_metadata.total_stages || 3}
                            </Typography>
                          </Box>
                        </Grid>
                        <Grid item xs={12}>
                          <Box>
                            <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 1 }}>
                              Description
                            </Typography>
                            <Typography variant="body2" sx={{ color: '#666' }}>
                              {selectedAssetDetails.pipeline_metadata.pipeline_description || 'Part of ETL/ELT data pipeline'}
                            </Typography>
                          </Box>
                        </Grid>
                      </Grid>
                    </CardContent>
                  </Card>
                </Box>

                {/* Pipeline Stage Visualization */}
                <Box sx={{ mb: 4 }}>
                  <Typography variant="subtitle1" sx={{ fontWeight: 600, mb: 3, color: '#333' }}>
                    Pipeline Flow
                  </Typography>
                  <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 2, flexWrap: 'wrap', mb: 3 }}>
                    {/* Extract Stage */}
                    <Box sx={{ 
                      p: 2, 
                      borderRadius: 2, 
                      border: selectedAssetDetails.pipeline_metadata.pipeline_stage === 'extract' ? '3px solid #1976d2' : '1px solid #ddd',
                      bgcolor: selectedAssetDetails.pipeline_metadata.pipeline_stage === 'extract' ? '#e3f2fd' : '#f9f9f9',
                      minWidth: 150,
                      textAlign: 'center'
                    }}>
                      <Typography variant="h6" sx={{ fontWeight: 600, mb: 1, color: selectedAssetDetails.pipeline_metadata.pipeline_stage === 'extract' ? '#1976d2' : '#666' }}>
                        1. EXTRACT
                      </Typography>
                      <Typography variant="caption" color="text.secondary">
                        Source → Staging
                      </Typography>
                      {selectedAssetDetails.pipeline_metadata.pipeline_stage === 'extract' && (
                        <Chip label="Current" size="small" color="primary" sx={{ mt: 1 }} />
                      )}
                    </Box>
                    
                    <ArrowForward sx={{ color: '#999', fontSize: 32 }} />
                    
                    {/* Transform Stage */}
                    <Box sx={{ 
                      p: 2, 
                      borderRadius: 2, 
                      border: selectedAssetDetails.pipeline_metadata.pipeline_stage === 'transform' ? '3px solid #1976d2' : '1px solid #ddd',
                      bgcolor: selectedAssetDetails.pipeline_metadata.pipeline_stage === 'transform' ? '#e3f2fd' : '#f9f9f9',
                      minWidth: 150,
                      textAlign: 'center'
                    }}>
                      <Typography variant="h6" sx={{ fontWeight: 600, mb: 1, color: selectedAssetDetails.pipeline_metadata.pipeline_stage === 'transform' ? '#1976d2' : '#666' }}>
                        2. TRANSFORM
                      </Typography>
                      <Typography variant="caption" color="text.secondary">
                        Staging → Warehouse
                      </Typography>
                      {selectedAssetDetails.pipeline_metadata.pipeline_stage === 'transform' && (
                        <Chip label="Current" size="small" color="primary" sx={{ mt: 1 }} />
                      )}
                    </Box>
                    
                    <ArrowForward sx={{ color: '#999', fontSize: 32 }} />
                    
                    {/* Load Stage */}
                    <Box sx={{ 
                      p: 2, 
                      borderRadius: 2, 
                      border: selectedAssetDetails.pipeline_metadata.pipeline_stage === 'load' ? '3px solid #1976d2' : '1px solid #ddd',
                      bgcolor: selectedAssetDetails.pipeline_metadata.pipeline_stage === 'load' ? '#e3f2fd' : '#f9f9f9',
                      minWidth: 150,
                      textAlign: 'center'
                    }}>
                      <Typography variant="h6" sx={{ fontWeight: 600, mb: 1, color: selectedAssetDetails.pipeline_metadata.pipeline_stage === 'load' ? '#1976d2' : '#666' }}>
                        3. LOAD
                      </Typography>
                      <Typography variant="caption" color="text.secondary">
                        Warehouse → Analytics
                      </Typography>
                      {selectedAssetDetails.pipeline_metadata.pipeline_stage === 'load' && (
                        <Chip label="Current" size="small" color="primary" sx={{ mt: 1 }} />
                      )}
                    </Box>
                  </Box>
                </Box>

                {/* Upstream Assets */}
                {selectedAssetDetails.pipeline_metadata.upstream_assets && selectedAssetDetails.pipeline_metadata.upstream_assets.length > 0 && (
                  <Box sx={{ mb: 4 }}>
                    <Typography variant="subtitle1" sx={{ fontWeight: 600, mb: 2, color: '#333' }}>
                      Upstream Assets in Pipeline
                    </Typography>
                    <Grid container spacing={2}>
                      {selectedAssetDetails.pipeline_metadata.upstream_assets.map((upstreamAsset, idx) => (
                        <Grid item xs={12} md={6} key={idx}>
                          <Card variant="outlined" sx={{ p: 2, cursor: 'pointer', '&:hover': { boxShadow: 2 } }}>
                            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'start' }}>
                              <Box>
                                <Typography variant="body1" sx={{ fontWeight: 600, mb: 0.5 }}>
                                  {upstreamAsset.name}
                                </Typography>
                                <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 1 }}>
                                  {upstreamAsset.id}
                                </Typography>
                                <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap' }}>
                                  <Chip label={upstreamAsset.stage} size="small" color="info" variant="outlined" />
                                  <Chip label={upstreamAsset.type} size="small" variant="outlined" />
                                </Box>
                              </Box>
                            </Box>
                          </Card>
                        </Grid>
                      ))}
                    </Grid>
                  </Box>
                )}

                {/* Downstream Assets */}
                {selectedAssetDetails.pipeline_metadata.downstream_assets && selectedAssetDetails.pipeline_metadata.downstream_assets.length > 0 && (
                  <Box sx={{ mb: 4 }}>
                    <Typography variant="subtitle1" sx={{ fontWeight: 600, mb: 2, color: '#333' }}>
                      Downstream Assets in Pipeline
                    </Typography>
                    <Grid container spacing={2}>
                      {selectedAssetDetails.pipeline_metadata.downstream_assets.map((downstreamAsset, idx) => (
                        <Grid item xs={12} md={6} key={idx}>
                          <Card variant="outlined" sx={{ p: 2, cursor: 'pointer', '&:hover': { boxShadow: 2 } }}>
                            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'start' }}>
                              <Box>
                                <Typography variant="body1" sx={{ fontWeight: 600, mb: 0.5 }}>
                                  {downstreamAsset.name}
                                </Typography>
                                <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 1 }}>
                                  {downstreamAsset.id}
                                </Typography>
                                <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap' }}>
                                  <Chip label={downstreamAsset.stage} size="small" color="success" variant="outlined" />
                                  <Chip label={downstreamAsset.type} size="small" variant="outlined" />
                                </Box>
                              </Box>
                            </Box>
                          </Card>
                        </Grid>
                      ))}
                    </Grid>
                  </Box>
                )}

                {/* Pipeline Statistics */}
                <Box>
                  <Typography variant="subtitle1" sx={{ fontWeight: 600, mb: 3, color: '#333' }}>
                    Pipeline Statistics
                  </Typography>
                  <Grid container spacing={3}>
                    <Grid item xs={12} md={4}>
                      <Box sx={{ p: 2, border: '1px solid #e0e0e0', borderRadius: 1, textAlign: 'center' }}>
                        <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 1 }}>
                          Total Pipeline Assets
                        </Typography>
                        <Typography variant="h4" sx={{ fontWeight: 600, color: '#1976d2' }}>
                          {(selectedAssetDetails.pipeline_metadata.upstream_assets?.length || 0) + 
                           (selectedAssetDetails.pipeline_metadata.downstream_assets?.length || 0) + 1}
                        </Typography>
                      </Box>
                    </Grid>
                    <Grid item xs={12} md={4}>
                      <Box sx={{ p: 2, border: '1px solid #e0e0e0', borderRadius: 1, textAlign: 'center' }}>
                        <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 1 }}>
                          Upstream Connections
                        </Typography>
                        <Typography variant="h4" sx={{ fontWeight: 600, color: '#00D4AA' }}>
                          {selectedAssetDetails.pipeline_metadata.upstream_assets?.length || 0}
                        </Typography>
                      </Box>
                    </Grid>
                    <Grid item xs={12} md={4}>
                      <Box sx={{ p: 2, border: '1px solid #e0e0e0', borderRadius: 1, textAlign: 'center' }}>
                        <Typography variant="body2" color="text.secondary" sx={{ fontSize: 12, fontWeight: 500, mb: 1 }}>
                          Downstream Connections
                        </Typography>
                        <Typography variant="h4" sx={{ fontWeight: 600, color: '#f57c00' }}>
                          {selectedAssetDetails.pipeline_metadata.downstream_assets?.length || 0}
                        </Typography>
                      </Box>
                    </Grid>
                  </Grid>
                </Box>
              </Box>
            )}
          </CardContent>
        </Card>
      )}

      {/* Asset Details Popup Dialog */}
      <Dialog
        open={detailsDialogOpen}
        onClose={handleCloseDialog}
        maxWidth="md"
        fullWidth
      >
        {selectedNode && (
          <>
            <DialogTitle>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <Box>
                  <Typography variant="h5" sx={{ fontWeight: 600 }}>
                    {selectedNode.name}
                  </Typography>
                  <Box sx={{ display: 'flex', gap: 1, mt: 1 }}>
                    <Chip label={selectedNode.type || 'Unknown'} size="small" color="primary" />
                    <Chip 
                      label={selectedNode.source_system || selectedNode.connector_id || 'Unknown'} 
                      size="small" 
                      variant="outlined"
                      sx={{ 
                        borderColor: '#999',
                        color: '#555',
                        backgroundColor: '#f5f5f5',
                        minWidth: '60px'
                      }}
                    />
                  </Box>
                </Box>
                <IconButton onClick={handleCloseDialog}>
                  <Close />
                </IconButton>
              </Box>
            </DialogTitle>
            <DialogContent dividers>
              {selectedNode.error ? (
                <Alert severity="error">{selectedNode.error}</Alert>
              ) : (
                <Grid container spacing={3}>
                  <Grid item xs={12}>
                    <Typography variant="subtitle2" color="text.secondary">
                      Asset ID
                    </Typography>
                    <Typography variant="body2" sx={{ wordBreak: 'break-all', fontFamily: 'monospace' }}>
                      {selectedNode.id}
                    </Typography>
                  </Grid>
                  <Grid item xs={6}>
                    <Typography variant="subtitle2" color="text.secondary">
                      Type
                    </Typography>
                    <Typography variant="body2">
                      {selectedNode.type}
                    </Typography>
                  </Grid>
                  <Grid item xs={6}>
                    <Typography variant="subtitle2" color="text.secondary">
                      Source System
                    </Typography>
                    <Typography variant="body2">
                      {selectedNode.source_system || selectedNode.connector_id || 'Unknown'}
                    </Typography>
                  </Grid>
                  <Grid item xs={12}>
                    <Typography variant="subtitle2" color="text.secondary">
                      Description
                    </Typography>
                    <Typography variant="body1">
                      {selectedNode.description || 'No description available'}
                    </Typography>
                  </Grid>
                  {selectedNode.columns && selectedNode.columns.length > 0 && (
                    <Grid item xs={12}>
                      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
                        <Typography variant="subtitle2" color="text.secondary">
                          Columns ({selectedNode.columns.length})
                        </Typography>
                        {selectedNode.columns.length > columnsPerPage && (
                          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                            <Button
                              size="small"
                              disabled={columnPage === 0}
                              onClick={() => setColumnPage(columnPage - 1)}
                            >
                              Previous
                            </Button>
                            <Typography variant="caption" color="text.secondary">
                              Page {columnPage + 1} of {Math.ceil(selectedNode.columns.length / columnsPerPage)}
                            </Typography>
                            <Button
                              size="small"
                              disabled={(columnPage + 1) * columnsPerPage >= selectedNode.columns.length}
                              onClick={() => setColumnPage(columnPage + 1)}
                            >
                              Next
                            </Button>
                          </Box>
                        )}
                      </Box>
                      <TableContainer component={Paper} variant="outlined">
                        <Table size="small">
                          <TableHead>
                            <TableRow>
                              <TableCell sx={{ fontWeight: 600, backgroundColor: '#f5f5f5' }}>Name</TableCell>
                              <TableCell sx={{ fontWeight: 600, backgroundColor: '#f5f5f5' }}>Type</TableCell>
                              <TableCell sx={{ fontWeight: 600, backgroundColor: '#f5f5f5' }}>PII</TableCell>
                              <TableCell sx={{ fontWeight: 600, backgroundColor: '#f5f5f5' }}>Description</TableCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            {selectedNode.columns
                              .slice(columnPage * columnsPerPage, (columnPage + 1) * columnsPerPage)
                              .map((col, index) => {
                              const isPII = detectPII(col.name, col.description);
                              return (
                              <TableRow key={index}>
                                  <TableCell sx={{ fontFamily: 'monospace', fontWeight: 500 }}>
                                    {col.name}
                                  </TableCell>
                                <TableCell>
                                    <Chip 
                                      label={col.type} 
                                      size="small" 
                                      variant="outlined"
                                      sx={{ borderColor: '#ddd', color: '#666' }}
                                    />
                                </TableCell>
                                  <TableCell>
                                    {isPII ? (
                                      <Chip 
                                        label="PII" 
                                        size="small" 
                                        color="error"
                                        sx={{ fontWeight: 600 }}
                                      />
                                    ) : (
                                      <Chip 
                                        label="Safe" 
                                        size="small" 
                                        color="success"
                                        variant="outlined"
                                        sx={{ fontWeight: 500 }}
                                      />
                                    )}
                                  </TableCell>
                                  <TableCell sx={{ color: '#666' }}>
                                    {col.description || '-'}
                                  </TableCell>
                              </TableRow>
                              );
                            })}
                          </TableBody>
                        </Table>
                      </TableContainer>
                      {selectedNode.columns.length > columnsPerPage && (
                        <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block' }}>
                          Showing {columnPage * columnsPerPage + 1}-{Math.min((columnPage + 1) * columnsPerPage, selectedNode.columns.length)} of {selectedNode.columns.length} columns
                        </Typography>
                      )}
                    </Grid>
                  )}
                </Grid>
              )}
            </DialogContent>
            <DialogActions>
              <Button onClick={handleCloseDialog}>Close</Button>
              <Button 
                variant="contained" 
                onClick={async () => {
                  try {
                    // Fetch full asset details from API
                    const response = await fetch(`http://localhost:8099/api/assets/${encodeURIComponent(selectedNode.id)}`);
                    if (response.ok) {
                      const data = await response.json();
                      setSelectedAssetDetails(data);
                      setActiveDetailTab('basic');
                      handleCloseDialog();
                    } else {
                      // Fallback to selectedNode if API fails
                      setSelectedAssetDetails(selectedNode);
                      setActiveDetailTab('basic');
                      handleCloseDialog();
                    }
                  } catch (error) {
                    console.error('Error fetching full asset details:', error);
                    // Fallback to selectedNode if API fails
                    setSelectedAssetDetails(selectedNode);
                    setActiveDetailTab('basic');
                    handleCloseDialog();
                  }
                }}
              >
                View Full Details
              </Button>
            </DialogActions>
          </>
        )}
      </Dialog>

      {/* Edge Details Dialog - Column Lineage */}
      <Dialog
        open={edgeDetailsOpen}
        onClose={handleCloseEdgeDialog}
        maxWidth="md"
        fullWidth
      >
        {selectedEdge && (
          <>
            <DialogTitle>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <Box>
                  <Typography variant="h5" sx={{ fontWeight: 600 }}>
                    Column-Level Lineage
                  </Typography>
                  <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
                    {selectedEdge.column_lineage.length} column relationships
                    {typeof selectedEdge.confidence_score === 'number' && (
                      <> • Confidence {(selectedEdge.confidence_score * 100).toFixed(1)}%</>
                    )}
                  </Typography>
                </Box>
                <IconButton onClick={handleCloseEdgeDialog}>
                  <Close />
                </IconButton>
              </Box>
            </DialogTitle>
            <DialogContent dividers>
              {selectedEdge.column_lineage && selectedEdge.column_lineage.length > 0 ? (
                <TableContainer component={Paper} variant="outlined">
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Source Column</TableCell>
                        <TableCell>→</TableCell>
                        <TableCell>Target Column</TableCell>
                        <TableCell>Relationship</TableCell>
                        <TableCell>Transformation</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {selectedEdge.column_lineage.map((colRel, index) => (
                        <TableRow key={index}>
                          <TableCell>
                            <Box>
                              <Typography variant="body2" sx={{ fontWeight: 600 }}>
                                {colRel.source_column}
                              </Typography>
                              <Typography variant="caption" color="text.secondary">
                                {colRel.source_table.split('.').pop()}
                              </Typography>
                            </Box>
                          </TableCell>
                          <TableCell>
                            <Box sx={{ display: 'flex', justifyContent: 'center' }}>
                              →
                            </Box>
                          </TableCell>
                          <TableCell>
                            <Box>
                              <Typography variant="body2" sx={{ fontWeight: 600 }}>
                                {colRel.target_column}
                              </Typography>
                              <Typography variant="caption" color="text.secondary">
                                {colRel.target_table.split('.').pop()}
                              </Typography>
                            </Box>
                          </TableCell>
                          <TableCell>
                            <Chip 
                              label={colRel.relationship_type.replace('_', ' ')} 
                              size="small" 
                              color={
                                colRel.relationship_type === 'direct_match' ? 'success' : 
                                colRel.relationship_type === 'transformed' ? 'warning' : 'info'
                              }
                            />
                          </TableCell>
                          <TableCell>
                            {colRel.transformation_type ? (
                              <Box>
                                <Chip 
                                  label={colRel.transformation_type} 
                                  size="small" 
                                  color="primary"
                                  variant="outlined"
                                  sx={{ mb: 0.5 }}
                                />
                                {colRel.transformation_expression && (
                                  <Typography variant="caption" sx={{ display: 'block', color: '#666', fontFamily: 'monospace' }}>
                                    {colRel.transformation_expression}
                                  </Typography>
                                )}
                              </Box>
                            ) : (
                              <Typography variant="caption" color="text.secondary">-</Typography>
                            )}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
              ) : (
                <Alert severity="info">
                  No column-level lineage information available for this relationship.
                </Alert>
              )}
            </DialogContent>
            <DialogActions>
              <Button onClick={handleCloseEdgeDialog}>Close</Button>
            </DialogActions>
          </>
        )}
      </Dialog>

      {/* Manual Lineage Dialog */}
      <ManualLineageDialog
        open={manualLineageOpen}
        onClose={() => setManualLineageOpen(false)}
        onSuccess={() => {
          fetchLineage();
        }}
      />
    </Box>
  );
};

export default DataLineagePage;

