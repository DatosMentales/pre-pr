/** @type {import('tailwindcss').Config} */
export default {
  content: ['./src/**/*.{html,js,svelte,ts}'],
  theme: {
    extend: {
      colors: {
        'primary': '#6d28d9',
        'primary-dark': '#5b21b6',
        'secondary': '#10b981',
        'secondary-dark': '#059669',
        'background': '#f3f4f6',
        'background-dark': '#111827',
        'foreground': '#1f2937',
        'foreground-dark': '#f9fafb',
        'card': '#ffffff',
        'card-dark': '#1f2937',
        'muted': '#6b7280',
        'muted-dark': '#9ca3af',
      }
    },
  },
  safelist: [
    'text-green-600',
    'text-red-600',
    'text-blue-600',
  ],
  plugins: [],
}