export const metadata = {
  title: 'AICaptions',
  description: 'Real Time Audio Captions using AI',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
