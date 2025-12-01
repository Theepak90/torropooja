import React, { useState, useRef, useEffect } from 'react';
import {
  Box,
  Paper,
  IconButton,
  TextField,
  Typography,
  Avatar,
  CircularProgress,
  Fade,
  Slide,
} from '@mui/material';
import {
  Chat as ChatIcon,
  Close as CloseIcon,
  Send as SendIcon,
  Psychology,
  SupportAgent as HeaderBotIcon,
} from '@mui/icons-material';
import ReactMarkdown from 'react-markdown';

// Use backend proxy to avoid CORS issues
const GEMINI_API_URL = 'http://localhost:8099/api/gemini/chat';

const AssetsChatbot = ({ assets = [] }) => {
  const [open, setOpen] = useState(false);
  const [messages, setMessages] = useState([
    {
      role: 'assistant',
      content: "Hi! I'm your AI assistant for the Torro Data Governance Platform. I can help you with:\n\n- **Assets**: Discovered data assets, their types, sources, and catalogs\n- **Connectors**: Status, configuration, and management of data source connectors\n- **System Health**: Monitoring status, connector health, and system metrics\n- **Catalog**: Documentation completeness and catalog management\n- **Platform Features**: How to use different parts of the platform\n- **Data Governance**: Best practices and guidance\n\nWhat would you like to know?",
    },
  ]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const messagesEndRef = useRef(null);
  const inputRef = useRef(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  useEffect(() => {
    if (open && inputRef.current) {
      inputRef.current.focus();
    }
  }, [open]);

  const [platformContext, setPlatformContext] = useState(null);

  // Fetch comprehensive platform context
  useEffect(() => {
    const fetchPlatformContext = async () => {
      try {
        const [statsRes, healthRes, connectorsRes, completenessRes] = await Promise.allSettled([
          fetch('http://localhost:8099/api/dashboard/stats'),
          fetch('http://localhost:8099/api/system/health'),
          fetch('http://localhost:8099/api/connectors'),
          fetch('http://localhost:8099/api/catalog/completeness'),
        ]);

        const context = {
          dashboardStats: statsRes.status === 'fulfilled' && statsRes.value.ok 
            ? await statsRes.value.json() : null,
          systemHealth: healthRes.status === 'fulfilled' && healthRes.value.ok
            ? await healthRes.value.json() : null,
          connectors: connectorsRes.status === 'fulfilled' && connectorsRes.value.ok
            ? await connectorsRes.value.json() : null,
          catalogCompleteness: completenessRes.status === 'fulfilled' && completenessRes.value.ok
            ? await completenessRes.value.json() : null,
        };

        setPlatformContext(context);
      } catch (error) {
        console.error('Error fetching platform context:', error);
      }
    };

    fetchPlatformContext();
    // Refresh context every 30 seconds
    const interval = setInterval(fetchPlatformContext, 30000);
    return () => clearInterval(interval);
  }, []);

  const getPlatformContext = () => {
    let context = '=== TORRO DATA GOVERNANCE PLATFORM CONTEXT ===\n\n';

    // Platform Overview
    context += 'PLATFORM OVERVIEW:\n';
    context += 'Torro is a comprehensive data governance and catalog platform that helps organizations discover, manage, and govern their data assets across multiple cloud providers and data sources.\n\n';

    // Dashboard Stats
    if (platformContext?.dashboardStats) {
      const stats = platformContext.dashboardStats;
      context += `DASHBOARD STATISTICS:\n`;
      context += `- Total Assets: ${stats.total_assets || 0}\n`;
      context += `- Total Catalogs: ${stats.total_catalogs || 0}\n`;
      context += `- Active Connectors: ${stats.active_connectors || 0}\n`;
      context += `- Monitoring Status: ${stats.monitoring_status || 'Unknown'}\n`;
      if (stats.last_scan) {
        context += `- Last Scan: ${stats.last_scan}\n`;
      }
      context += '\n';
    }

    // System Health
    if (platformContext?.systemHealth) {
      const health = platformContext.systemHealth;
      context += `SYSTEM HEALTH:\n`;
      context += `- Status: ${health.status || 'Unknown'}\n`;
      context += `- Monitoring Enabled: ${health.monitoring_enabled ? 'Yes' : 'No'}\n`;
      context += `- Connectors Enabled: ${health.connectors_enabled || 0} / ${health.connectors_total || 0}\n`;
      if (health.last_scan) {
        context += `- Last Scan: ${health.last_scan}\n`;
      }
      context += '\n';
    }

    // Connectors Information
    if (platformContext?.connectors) {
      const connectors = Array.isArray(platformContext.connectors) 
        ? platformContext.connectors 
        : (platformContext.connectors.connectors || []);
      
      if (connectors.length > 0) {
        context += `CONNECTORS (${connectors.length} total):\n`;
        connectors.forEach((conn, idx) => {
          context += `${idx + 1}. ${conn.name || 'Unnamed'} (${conn.type || 'Unknown'})\n`;
          context += `   - Status: ${conn.status || 'Unknown'}\n`;
          context += `   - Enabled: ${conn.enabled ? 'Yes' : 'No'}\n`;
          if (conn.last_run) {
            context += `   - Last Run: ${conn.last_run}\n`;
          }
        });
        context += '\n';
      }
    }

    // Catalog Completeness
    if (platformContext?.catalogCompleteness) {
      const completeness = platformContext.catalogCompleteness;
      context += `CATALOG COMPLETENESS:\n`;
      context += `- Overall Completeness: ${(completeness.overall_completeness * 100).toFixed(1)}%\n`;
      context += `- Total Entities: ${completeness.total_entities || 0}\n`;
      context += `- Total Documented: ${completeness.total_documented || 0}\n`;
      if (completeness.by_entity_type) {
        context += `- By Entity Type:\n`;
        Object.entries(completeness.by_entity_type).forEach(([type, data]) => {
          const percentage = data.completeness ? (data.completeness * 100).toFixed(1) : '0.0';
          context += `  * ${type}: ${percentage}% (${data.documented || 0}/${data.total || 0})\n`;
        });
      }
      context += '\n';
    }

    // Assets Information
    if (assets && assets.length > 0) {
      const totalAssets = assets.length;
      const assetTypes = {};
      const dataSources = {};
      const catalogs = {};

      assets.forEach((asset) => {
        assetTypes[asset.type] = (assetTypes[asset.type] || 0) + 1;
        
        const source = asset.connector_id?.startsWith('bq_') ? 'BigQuery' :
                      asset.connector_id?.startsWith('starburst_') ? 'Starburst Galaxy' :
                      asset.connector_id?.startsWith('s3_') ? 'Amazon S3' :
                      asset.connector_id?.startsWith('gcs_') ? 'Google Cloud Storage' :
                      asset.connector_id?.startsWith('azure_blob_') ? 'Azure Blob Storage' :
                      asset.connector_id?.startsWith('azure_files_') ? 'Azure Files' :
                      asset.connector_id?.startsWith('azure_tables_queues_') ? 'Azure Tables & Queues' :
                      'Unknown';
        dataSources[source] = (dataSources[source] || 0) + 1;
        
        if (asset.catalog) {
          catalogs[asset.catalog] = (catalogs[asset.catalog] || 0) + 1;
        }
      });

      context += `DISCOVERED ASSETS (Current Page: ${totalAssets} assets):\n`;
      
      const topTypes = Object.entries(assetTypes)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 5)
        .map(([type, count]) => `${type} (${count})`)
        .join(', ');

      const topSources = Object.entries(dataSources)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 5)
        .map(([source, count]) => `${source} (${count})`)
        .join(', ');

      const topCatalogs = Object.entries(catalogs)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 5)
        .map(([catalog, count]) => `${catalog} (${count})`)
        .join(', ');

      context += `- Top Asset Types: ${topTypes || 'N/A'}\n`;
      context += `- Top Data Sources: ${topSources || 'N/A'}\n`;
      context += `- Top Catalogs: ${topCatalogs || 'N/A'}\n`;
      
      if (assets.length <= 10) {
        context += `\nRecent Assets:\n`;
        assets.slice(0, 10).forEach((asset, idx) => {
          context += `${idx + 1}. ${asset.name} (${asset.type}) - ${asset.catalog || 'N/A'}\n`;
        });
      }
      context += '\n';
    } else {
      context += `DISCOVERED ASSETS: No assets currently visible on this page.\n\n`;
    }

    // Platform Features
    context += `PLATFORM FEATURES:\n`;
    context += `- Asset Discovery: Automatically discovers data assets from multiple sources\n`;
    context += `- Multi-Cloud Support: BigQuery, Starburst Galaxy, Amazon S3, Google Cloud Storage, Azure Blob Storage, Azure Files, Azure Tables & Queues\n`;
    context += `- Data Lineage: Track data flow and dependencies\n`;
    context += `- Catalog Management: Organize and document data assets\n`;
    context += `- Governance Controls: Manage access, classification, and compliance\n`;
    context += `- Real-time Monitoring: Track system health and connector status\n`;
    context += `- Marketplace: Discover and share data assets\n\n`;

    context += `=== END CONTEXT ===\n`;

    return context;
  };

  const sendMessage = async () => {
    if (!input.trim() || loading) return;

    const userMessage = input.trim();
    setInput('');
    setMessages((prev) => [...prev, { role: 'user', content: userMessage }]);
    setLoading(true);

    try {
      const platformContext = getPlatformContext();
      
      const prompt = `You are an AI assistant for the Torro Data Governance Platform. You help users understand and navigate all aspects of the platform including:

- Discovered assets and data sources
- Connectors and their status
- System health and monitoring
- Catalog completeness and documentation
- Platform features and capabilities
- Data lineage and governance
- How to use different parts of the platform

${platformContext}

The user is asking: "${userMessage}"

Please provide a helpful, comprehensive response based on the platform context provided. You can answer questions about:
- Assets, connectors, system status, catalog completeness
- How to use platform features
- Troubleshooting issues
- Understanding platform metrics and statistics
- General questions about data governance and the platform

Use the context information provided above to give accurate, specific answers. If information isn't available in the context, acknowledge that and suggest where the user might find it in the platform interface. Keep responses clear, actionable, and well-formatted with proper markdown.`;

      const response = await fetch(GEMINI_API_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          prompt: prompt,
        }),
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        console.error('Gemini API error:', {
          status: response.status,
          statusText: response.statusText,
          error: errorData,
        });
        throw new Error(`API error: ${response.status} - ${errorData.error || response.statusText}`);
      }

      const data = await response.json();
      const botResponse = data.response || data.error || "I'm sorry, I couldn't generate a response. Please try again.";

      setMessages((prev) => [
        ...prev,
        { role: 'assistant', content: botResponse },
      ]);
    } catch (error) {
      console.error('Error calling Gemini API:', error);
      const errorMessage = error.message?.includes('API error') 
        ? `I'm sorry, I encountered an API error: ${error.message}. Please check your API key and try again.`
        : "I'm sorry, I encountered an error. Please check your connection and try again.";
      setMessages((prev) => [
        ...prev,
        {
          role: 'assistant',
          content: errorMessage,
        },
      ]);
    } finally {
      setLoading(false);
    }
  };

  const handleKeyPress = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  };

  return (
    <>
      {/* Chat Button */}
      <Fade in={!open}>
        <Box
          sx={{
            position: 'fixed',
            bottom: 24,
            right: 24,
            zIndex: 1000,
          }}
        >
          <IconButton
            onClick={() => setOpen(true)}
            sx={{
              backgroundColor: 'primary.main',
              color: 'white',
              width: 64,
              height: 64,
              boxShadow: '0 4px 12px rgba(0, 0, 0, 0.15)',
              '&:hover': {
                backgroundColor: 'primary.dark',
                transform: 'scale(1.05)',
              },
              transition: 'all 0.3s ease',
            }}
          >
            <ChatIcon sx={{ fontSize: 32 }} />
          </IconButton>
        </Box>
      </Fade>

      {/* Chat Window */}
      <Slide direction="up" in={open} mountOnEnter unmountOnExit>
        <Paper
          elevation={8}
          sx={{
            position: 'fixed',
            bottom: 24,
            right: 24,
            width: 400,
            height: 600,
            maxHeight: 'calc(100vh - 48px)',
            display: 'flex',
            flexDirection: 'column',
            zIndex: 1001,
            borderRadius: 2,
            overflow: 'hidden',
          }}
        >
          {/* Header */}
          <Box
            sx={{
              backgroundColor: 'primary.main',
              color: 'white',
              p: 2,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
            }}
          >
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <HeaderBotIcon sx={{ fontSize: 24 }} />
              <Typography variant="h6" sx={{ fontWeight: 600 }}>
                Assets Assistant
              </Typography>
            </Box>
            <IconButton
              onClick={() => setOpen(false)}
              sx={{ color: 'white' }}
              size="small"
            >
              <CloseIcon />
            </IconButton>
          </Box>

          {/* Messages */}
          <Box
            sx={{
              flex: 1,
              overflowY: 'auto',
              p: 2,
              backgroundColor: '#f5f5f5',
              display: 'flex',
              flexDirection: 'column',
              gap: 2,
            }}
          >
            {messages.map((message, index) => (
              <Box
                key={index}
                sx={{
                  display: 'flex',
                  justifyContent:
                    message.role === 'user' ? 'flex-end' : 'flex-start',
                  gap: 1,
                }}
              >
                {message.role === 'assistant' && (
                  <Avatar
                    sx={{
                      backgroundColor: 'primary.main',
                      width: 32,
                      height: 32,
                    }}
                  >
                    <Psychology sx={{ fontSize: 20 }} />
                  </Avatar>
                )}
                <Paper
                  elevation={1}
                  sx={{
                    p: 1.5,
                    maxWidth: '75%',
                    backgroundColor:
                      message.role === 'user'
                        ? 'primary.main'
                        : 'background.paper',
                    color:
                      message.role === 'user' ? 'white' : 'text.primary',
                    borderRadius: 2,
                  }}
                >
                  {message.role === 'assistant' ? (
                    <ReactMarkdown
                      components={{
                        p: ({ children }) => (
                          <Typography variant="body2" component="p" sx={{ mb: 1, '&:last-child': { mb: 0 } }}>
                            {children}
                          </Typography>
                        ),
                        strong: ({ children }) => (
                          <Typography component="strong" sx={{ fontWeight: 600, color: 'inherit' }}>
                            {children}
                          </Typography>
                        ),
                        em: ({ children }) => (
                          <Typography component="em" sx={{ fontStyle: 'italic', color: 'inherit' }}>
                            {children}
                          </Typography>
                        ),
                        code: ({ inline, children }) => (
                          inline ? (
                            <Typography
                              component="code"
                              sx={{
                                backgroundColor: 'rgba(0, 0, 0, 0.08)',
                                padding: '2px 4px',
                                borderRadius: '4px',
                                fontFamily: 'monospace',
                                fontSize: '0.85em',
                              }}
                            >
                              {children}
                            </Typography>
                          ) : (
                            <Box
                              component="code"
                              sx={{
                                display: 'block',
                                backgroundColor: 'rgba(0, 0, 0, 0.05)',
                                padding: '8px',
                                borderRadius: '4px',
                                fontFamily: 'monospace',
                                fontSize: '0.85em',
                                overflow: 'auto',
                                mb: 1,
                              }}
                            >
                              {children}
                            </Box>
                          )
                        ),
                        ul: ({ children }) => (
                          <Box component="ul" sx={{ pl: 2, mb: 1, '&:last-child': { mb: 0 } }}>
                            {children}
                          </Box>
                        ),
                        ol: ({ children }) => (
                          <Box component="ol" sx={{ pl: 2, mb: 1, '&:last-child': { mb: 0 } }}>
                            {children}
                          </Box>
                        ),
                        li: ({ children }) => (
                          <Typography component="li" variant="body2" sx={{ mb: 0.5 }}>
                            {children}
                          </Typography>
                        ),
                      }}
                    >
                      {message.content}
                    </ReactMarkdown>
                  ) : (
                    <Typography variant="body2" sx={{ whiteSpace: 'pre-wrap' }}>
                      {message.content}
                    </Typography>
                  )}
                </Paper>
                {message.role === 'user' && (
                  <Avatar
                    sx={{
                      backgroundColor: 'secondary.main',
                      width: 32,
                      height: 32,
                    }}
                  >
                    U
                  </Avatar>
                )}
              </Box>
            ))}
            {loading && (
              <Box sx={{ display: 'flex', justifyContent: 'flex-start', gap: 1 }}>
                <Avatar
                  sx={{
                    backgroundColor: 'primary.main',
                    width: 32,
                    height: 32,
                  }}
                >
                  <Psychology sx={{ fontSize: 20 }} />
                </Avatar>
                <Paper
                  elevation={1}
                  sx={{
                    p: 1.5,
                    backgroundColor: 'background.paper',
                    borderRadius: 2,
                  }}
                >
                  <CircularProgress size={16} />
                </Paper>
              </Box>
            )}
            <div ref={messagesEndRef} />
          </Box>

          {/* Input */}
          <Box
            sx={{
              p: 2,
              borderTop: '1px solid',
              borderColor: 'divider',
              backgroundColor: 'background.paper',
            }}
          >
            <Box sx={{ display: 'flex', gap: 1 }}>
              <TextField
                inputRef={inputRef}
                fullWidth
                size="small"
                placeholder="Ask about your assets..."
                value={input}
                onChange={(e) => setInput(e.target.value)}
                onKeyPress={handleKeyPress}
                disabled={loading}
                sx={{
                  '& .MuiOutlinedInput-root': {
                    borderRadius: 2,
                  },
                }}
              />
              <IconButton
                onClick={sendMessage}
                disabled={!input.trim() || loading}
                color="primary"
                sx={{
                  backgroundColor: 'primary.main',
                  color: 'white',
                  '&:hover': {
                    backgroundColor: 'primary.dark',
                  },
                  '&:disabled': {
                    backgroundColor: 'action.disabledBackground',
                    color: 'action.disabled',
                  },
                }}
              >
                <SendIcon />
              </IconButton>
            </Box>
          </Box>
        </Paper>
      </Slide>
    </>
  );
};

export default AssetsChatbot;

