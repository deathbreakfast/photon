//! Latency-derived metrics.

#[derive(Debug, Clone, Copy)]
pub struct PublishSlope {
    pub slope: f64,
}

/// Linear slope of publish latency (ms) vs op index.
pub fn publish_slope_vs_index(samples: &[f64]) -> PublishSlope {
    if samples.len() < 2 {
        return PublishSlope { slope: 0.0 };
    }
    #[allow(clippy::cast_precision_loss)]
    let n = samples.len() as f64;
    let mean_x = (n - 1.0) / 2.0;
    let mean_y = samples.iter().sum::<f64>() / n;
    let mut num = 0.0;
    let mut den = 0.0;
    for (i, y) in samples.iter().enumerate() {
        #[allow(clippy::cast_precision_loss)]
        let x = i as f64;
        num = (x - mean_x).mul_add(y - mean_y, num);
        den += (x - mean_x).powi(2);
    }
    PublishSlope {
        slope: if den.abs() < f64::EPSILON {
            0.0
        } else {
            num / den
        },
    }
}
