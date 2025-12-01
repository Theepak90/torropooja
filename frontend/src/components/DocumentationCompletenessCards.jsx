import React, { useState, useEffect } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  LinearProgress,
  Grid,
  Chip,
  CircularProgress,
  Alert,
  Tooltip,
} from '@mui/material';
import {
  DataObject,
  Storage,
  TableChart,
  Assessment,
  Description,
  Article,
  TrendingUp,
  TrendingDown,
} from '@mui/icons-material';

const DocumentationCompletenessCards = () => {
  const [completeness, setCompleteness] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    fetchCompleteness();
    // Refresh every 5 minutes
    const interval = setInterval(fetchCompleteness, 5 * 60 * 1000);
    return () => clearInterval(interval);
  }, []);

  const fetchCompleteness = async () => {
    try {
      setLoading(true);
      const response = await fetch('http://localhost:8099/api/catalog/completeness');
      if (!response.ok) {
        throw new Error('Failed to fetch completeness data');
      }
      const data = await response.json();
      setCompleteness(data);
      setError(null);
    } catch (err) {
      setError(err.message);
      console.error('Error fetching completeness:', err);
    } finally {
      setLoading(false);
    }
  };

  const getEntityIcon = (entityType) => {
    const icons = {
      data_sources: <Storage />,
      schemas: <DataObject />,
      tables: <TableChart />,
      reports: <Assessment />,
      queries: <Description />,
      articles: <Article />,
    };
    return icons[entityType] || <DataObject />;
  };

  const getEntityLabel = (entityType) => {
    const labels = {
      data_sources: 'Data Sources',
      schemas: 'Schemas',
      tables: 'Tables',
      reports: 'Reports',
      queries: 'Queries',
      articles: 'Articles',
    };
    return labels[entityType] || entityType;
  };

  const getColorForPercentage = (percentage) => {
    if (percentage >= 80) return 'success';
    if (percentage >= 50) return 'warning';
    return 'error';
  };

  if (loading) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', p: 4 }}>
        <CircularProgress />
      </Box>
    );
  }

  if (error) {
    return (
      <Alert severity="error" sx={{ m: 2 }}>
        Failed to load documentation completeness: {error}
      </Alert>
    );
  }

  if (!completeness) {
    return null;
  }

  const { overall_completeness, by_entity_type } = completeness;

  return (
    <Box sx={{ p: 2 }}>
      {/* Overall Completeness Card */}
      <Card sx={{ mb: 3, background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)', color: 'white' }}>
        <CardContent>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
            <Typography variant="h5" sx={{ fontWeight: 600 }}>
              Overall Documentation Completeness
            </Typography>
            <Chip
              label={`${overall_completeness.toFixed(1)}%`}
              sx={{
                backgroundColor: 'rgba(255, 255, 255, 0.2)',
                color: 'white',
                fontSize: '1.2rem',
                fontWeight: 600,
                height: 40,
              }}
            />
          </Box>
          <LinearProgress
            variant="determinate"
            value={overall_completeness}
            sx={{
              height: 12,
              borderRadius: 6,
              backgroundColor: 'rgba(255, 255, 255, 0.3)',
              '& .MuiLinearProgress-bar': {
                backgroundColor: 'white',
              },
            }}
          />
          <Box sx={{ display: 'flex', justifyContent: 'space-between', mt: 2 }}>
            <Typography variant="body2">
              {completeness.total_documented} of {completeness.total_entities} entities documented
            </Typography>
            <Typography variant="body2">
              {completeness.total_entities - completeness.total_documented} remaining
            </Typography>
          </Box>
        </CardContent>
      </Card>

      {/* Entity Type Cards */}
      <Grid container spacing={2}>
        {Object.entries(by_entity_type).map(([entityType, metrics]) => {
          const percentage = metrics.completeness_percentage;
          const color = getColorForPercentage(percentage);
          
          return (
            <Grid item xs={12} sm={6} md={4} key={entityType}>
              <Card
                sx={{
                  height: '100%',
                  transition: 'transform 0.2s, box-shadow 0.2s',
                  '&:hover': {
                    transform: 'translateY(-4px)',
                    boxShadow: 4,
                  },
                }}
              >
                <CardContent>
                  <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      {getEntityIcon(entityType)}
                      <Typography variant="h6" sx={{ fontWeight: 600 }}>
                        {getEntityLabel(entityType)}
                      </Typography>
                    </Box>
                    <Chip
                      label={`${percentage.toFixed(1)}%`}
                      color={color}
                      size="small"
                      sx={{ fontWeight: 600 }}
                    />
                  </Box>

                  <LinearProgress
                    variant="determinate"
                    value={percentage}
                    color={color}
                    sx={{ height: 8, borderRadius: 4, mb: 2 }}
                  />

                  <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                    <Box sx={{ display: 'flex', justifyContent: 'space-between' }}>
                      <Typography variant="body2" color="text.secondary">
                        Total:
                      </Typography>
                      <Typography variant="body2" sx={{ fontWeight: 600 }}>
                        {metrics.total}
                      </Typography>
                    </Box>
                    <Box sx={{ display: 'flex', justifyContent: 'space-between' }}>
                      <Typography variant="body2" color="text.secondary">
                        Documented:
                      </Typography>
                      <Typography variant="body2" sx={{ fontWeight: 600, color: 'success.main' }}>
                        {metrics.documented}
                      </Typography>
                    </Box>
                    <Box sx={{ display: 'flex', justifyContent: 'space-between' }}>
                      <Typography variant="body2" color="text.secondary">
                        Missing:
                      </Typography>
                      <Typography variant="body2" sx={{ fontWeight: 600, color: 'error.main' }}>
                        {metrics.missing_count}
                      </Typography>
                    </Box>
                  </Box>

                  {Object.keys(metrics.missing_fields).length > 0 && (
                    <Box sx={{ mt: 2, pt: 2, borderTop: 1, borderColor: 'divider' }}>
                      <Typography variant="caption" color="text.secondary" sx={{ mb: 1, display: 'block' }}>
                        Missing Fields:
                      </Typography>
                      <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                        {Object.entries(metrics.missing_fields)
                          .filter(([_, count]) => count > 0)
                          .map(([field, count]) => (
                            <Tooltip key={field} title={`${count} assets missing ${field}`}>
                              <Chip
                                label={`${field}: ${count}`}
                                size="small"
                                variant="outlined"
                                color="error"
                                sx={{ fontSize: '0.7rem' }}
                              />
                            </Tooltip>
                          ))}
                      </Box>
                    </Box>
                  )}
                </CardContent>
              </Card>
            </Grid>
          );
        })}
      </Grid>
    </Box>
  );
};

export default DocumentationCompletenessCards;




