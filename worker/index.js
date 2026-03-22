/**
 * Gigante Intelligence — Claude AI Chat Proxy
 *
 * Cloudflare Worker that proxies chat requests to Anthropic's Claude API.
 * Keeps the API key secure (stored as a Cloudflare secret, never exposed to browser).
 *
 * Deploy: https://dash.cloudflare.com → Workers & Pages → Create → "gigante-ai"
 * Secret: ANTHROPIC_API_KEY (set via Workers dashboard or `wrangler secret put ANTHROPIC_API_KEY`)
 *
 * Usage from dashboard:
 *   POST https://gigante-ai.<your-subdomain>.workers.dev/chat
 *   Body: { "question": "...", "context": { ... dashboard data summary ... } }
 */

const SYSTEM_PROMPT = `You are Gigante Intelligence, the AI analytics assistant for Gigante Restaurant & Bar in Eastchester, NY. You have deep knowledge of the business:

BUSINESS CONTEXT:
- Gigante Restaurant & Bar: Upscale-casual Italian dining that transitions to nightlife (Thu-Sat)
- Brand positioning: "Dinner with a scene" — luxury social dining in Westchester
- Parent company: Eastchester Events Inc. / Gigante Hospitality
- Sister venues: Mulino's at Lake Isle (20K sq ft catering, 50-500 guests), Snack Shack (seasonal)
- Location: Town-owned Lake Isle Country Club grounds, Eastchester, NY
- Hours: Tue-Wed 4-10PM, Thu-Fri 4PM-1AM, Sat 5PM-1AM, Sun Brunch 11AM-3PM, Mon Closed
- Target audience: Modern affluent socializers (28-45), Westchester + NYC bridge-and-tunnel
- Tech stack: Toast POS, SevenRooms reservations, Tripleseat events, Instagram, Mailchimp

BRAND VOICE RULES:
- Confident, understated, Apple-level brevity
- NEVER use: gourmet, luxurious, indulge, delectable, curated, bespoke
- Food-led always — nightlife supports, never leads
- Tone: "Tao meets Scarsdale"

RESPONSE STYLE:
- You are talking to Lou, the owner/operator. Be direct, strategic, data-driven.
- Give advanced tactics only — skip hospitality basics.
- When analyzing data, always tie back to revenue impact and actionable next steps.
- Use bullet points for action items, paragraphs for analysis.
- If asked about marketing copy, follow the brand voice rules above.
- Reference specific data points from the context provided.
- Keep responses concise but thorough — Lou has ADHD, so scannable formatting matters.`;

export default {
  async fetch(request, env) {
    // CORS headers for GitHub Pages
    const corsHeaders = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    };

    // Handle CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }

    // Only accept POST to /chat
    const url = new URL(request.url);
    if (request.method !== 'POST' || url.pathname !== '/chat') {
      return new Response(JSON.stringify({ error: 'POST /chat only' }), {
        status: 404,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }

    try {
      const { question, context } = await request.json();

      if (!question || typeof question !== 'string') {
        return new Response(JSON.stringify({ error: 'Missing question' }), {
          status: 400,
          headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }

      // Build the user message with dashboard context
      let userMessage = question;
      if (context) {
        userMessage = `CURRENT DASHBOARD DATA (${context.window || 'Last 30 Days'}):\n${JSON.stringify(context, null, 0)}\n\nQUESTION: ${question}`;
      }

      // Call Claude API
      const response = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-api-key': env.ANTHROPIC_API_KEY,
          'anthropic-version': '2023-06-01',
        },
        body: JSON.stringify({
          model: 'claude-sonnet-4-20250514',
          max_tokens: 1024,
          system: SYSTEM_PROMPT,
          messages: [{ role: 'user', content: userMessage }],
        }),
      });

      if (!response.ok) {
        const errText = await response.text();
        console.error('Anthropic API error:', response.status, errText);
        return new Response(JSON.stringify({ error: 'AI service error', status: response.status }), {
          status: 502,
          headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }

      const data = await response.json();
      const reply = data.content?.[0]?.text || 'No response generated.';

      return new Response(JSON.stringify({ reply }), {
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });

    } catch (err) {
      console.error('Worker error:', err);
      return new Response(JSON.stringify({ error: 'Internal error' }), {
        status: 500,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }
  }
};
